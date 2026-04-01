"""An Elixir/Erlang-GenServer-like actor."""

from __future__ import annotations

import asyncio
from asyncio import QueueShutDown, Task, TaskGroup
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from aio_sync.mpmc import MPMC, MPMCReceiver, MPMCSender, mpmc_channel
from aio_sync.oneshot import OneShot, OneShotSender


@dataclass(slots=True)
class MsgCall[Call, Reply]:
    """A "call" message (request-reply)."""

    msg: Call
    reply_sender: OneShotSender[Reply | QueueShutDown]


@dataclass(slots=True, frozen=True)
class TrackedOneShotSender[T]:
    """Same as `aio_sync.oneshot.OneShotSender`, but tracked by an `Actor` to
    guarantee the receiver gets `QueueShutDown` in case the actor exits."""

    sender_id: int
    registry: _ReplyRegistry[T]

    def send(self, value: T):
        """Send a value through the one-shot channel, immediately.

        @raise ValueError if sending more than once.
        Also marks the sender as done in the `Actor`'s registry.
        Examples:

        >>> sender, receiver = OneShot[int].channel()
        >>> sender.send(4)
        >>> receiver.try_recv()
        4
        """
        return self.registry.send(self.sender_id, value)

    def inner_no_unregister(self) -> OneShotSender[T | QueueShutDown]:
        """Take out the inner `OneShotSender` without unregistering it from
        the registry. Useful for using with `relay_call`.
        @raise KeyError if `self.sender_id` not tracked."""
        return self.registry.in_flight[self.sender_id]

    def inner_unregister(self) -> OneShotSender[T | QueueShutDown]:
        """Take out the inner `OneShotSender` and unregister it from
        the registry.
        Use `inner_no_unregister` instead if using with `relay_call`.
        @raise KeyError if `self.sender_id` not tracked."""
        return self.registry.in_flight.pop(self.sender_id)


@dataclass(slots=True)
class _ReplyRegistry[Reply]:
    """Keeps track of in-flight `TrackedOneShotSender`s for an `Actor` and
    allows sending replies to them."""

    in_flight: dict[int, OneShotSender[Reply | QueueShutDown]] = field(
        default_factory=dict
    )
    is_shut_down: bool = False

    def register(
        self, reply_sender: OneShotSender[Reply | QueueShutDown]
    ) -> TrackedOneShotSender[Reply]:
        """Register a `OneShotSender` to track it and
        guarantee the receiver gets `QueueShutDown` in case the actor exits.
        @raise AssertionError if registry is shut down."""
        assert not self.is_shut_down, (
            "Cannot register new reply sender after registry is shut down.",
            self,
        )
        sender_id = id(reply_sender)
        self.in_flight[sender_id] = reply_sender
        return TrackedOneShotSender(sender_id, self)

    def send(self, sender_id: int, value: Reply | QueueShutDown):
        """Send a value through the registered one-shot channel.
        @raise KeyError if `sender_id` not tracked.
        @raise ValueError if sending more than once."""
        sender = self.in_flight.pop(sender_id, None)
        if sender is None:
            raise KeyError(
                "Got non-tracked sender ID! Hint: check if you are sending more than once.",
                sender_id,
                self,
            )
        return sender.send(value)

    def shutdown(self) -> None:
        """Shut down the registry and send `QueueShutDown("Actor exited.")` to all
        in-flight senders."""
        if self.is_shut_down:
            return
        self.is_shut_down = True
        for reply_sender in self.in_flight.values():
            try:
                reply_sender.send(QueueShutDown("Actor exited."))
            except ValueError:
                pass
        self.in_flight.clear()


@dataclass(slots=True)
class MsgCast[Cast]:
    """A "cast" message (fire-and-forget)."""

    msg: Cast


type Msg[Call, Cast, Reply] = MsgCall[Call, Reply] | MsgCast[Cast]
"""A message sent to an actor."""


def _reply_shutdown_to_queued_calls[Call, Cast, Reply](
    msg_receiver: MPMCReceiver[Msg[Call, Cast, Reply]],
) -> None:
    inner_queue: deque[Msg[Call, Cast, Reply]] | None = getattr(
        msg_receiver._queue, "_queue", None
    )
    assert isinstance(inner_queue, deque), (
        "MPMC receiver queue internals changed; cannot inspect queued messages.",
        msg_receiver,
    )
    for queued_msg in inner_queue:
        match queued_msg:
            case MsgCall(reply_sender=reply_sender):
                try:
                    reply_sender.send(QueueShutDown("Actor exited."))
                except ValueError:
                    pass
            case MsgCast():
                pass


@dataclass(slots=True)
class ActorRunResult[Call, Cast, Reply]:
    """The result when the `Actor` exits."""

    actor: Actor[Call, Cast, Reply]
    env: ActorEnv[Call, Cast, Reply]
    exit_result: Exception | None


@dataclass(slots=True)
class Env[Call, Cast, Reply]:
    """The environment the `Actor` runs in."""

    ref_: ActorRef[Call, Cast, Reply]
    msg_receiver: MPMCReceiver[Msg[Call, Cast, Reply]]
    reply_registry: _ReplyRegistry[Reply] = field(default_factory=_ReplyRegistry)


type ActorEnv[Call, Cast, Reply] = Env[Call, Cast, Reply]
"""The environment the `Actor` runs in."""


@dataclass(slots=True)
class ActorRef[Call, Cast, Reply]:
    """A reference to an instance of `Actor`, to cast or call messages on it."""

    msg_sender: MPMCSender[Msg[Call, Cast, Reply]]
    actor_task: Task[ActorRunResult[Call, Cast, Reply]] | None = None

    async def cast(self, msg: Cast) -> QueueShutDown | None:
        """Cast a message to the actor and do not expect a reply."""
        return await self.msg_sender.send(MsgCast(msg))

    async def call(self, msg: Call) -> Reply | QueueShutDown:
        """Call the actor and wait for a reply.
        To time out the call, use `asyncio.wait_for`."""
        if self.actor_task is not None and (
            self.actor_task.done() or self.actor_task.cancelling()
        ):
            return QueueShutDown("Actor exited.")
        reply_sender, reply_receiver = OneShot[Reply | QueueShutDown].channel()
        send_err = await self.msg_sender.send(MsgCall(msg, reply_sender))
        if send_err is not None:
            return send_err
        # NOTE: Previously we were using
        # `asyncio.wait({recv_task,self.actor_task})`, but
        # it caused memory leaks bc awaiting on a task in
        # Python 3.14 causes the task to track the awaiter.
        return await reply_receiver.recv()

    async def relay_call(
        self, msg: Call, reply_sender: TrackedOneShotSender[Reply]
    ) -> QueueShutDown | None:
        """Call the actor and let it reply via a given one-shot sender.
        Useful for relaying a call from some other caller."""
        return await self.msg_sender.send(
            MsgCall(msg, reply_sender.inner_no_unregister())
        )

    def cancel(self) -> bool:
        """Cancel the actor referred to, so it exits, and does not wait for
        it to exit.
        @return True if the task was cancelled, False if it already finished or
        never started."""
        if self.actor_task is None:
            return False
        return self.actor_task.cancel()


class Actor[Call, Cast, Reply](Protocol):
    """An Elixir/Erlang-GenServer-like actor"""

    async def init(self, _env: ActorEnv[Call, Cast, Reply]) -> Exception | None:
        """Called when the actor starts.
        # Snippet for copying
        ```py
        async def init(self, env: ActorEnv[Call, Cast, Reply]) -> None:
            return
        ```
        """
        return

    async def handle_cast(
        self, _msg: Cast, _env: ActorEnv[Call, Cast, Reply]
    ) -> Exception | None:
        """Called when the actor receives a message and does not need to reply.
        # Snippet for copying
        ```py
        async def handle_cast(self, msg: Cast, env: ActorEnv[Call, Cast, Reply]) -> None:
            return
        ```
        """
        return

    async def handle_call(
        self,
        _msg: Call,
        _env: ActorEnv[Call, Cast, Reply],
        _reply_sender: TrackedOneShotSender[Reply],
    ) -> Exception | None:
        """Called when the actor receives a message and needs to reply.

        Implementations should send exactly one reply using `reply_sender`.
        If the actor exits before sending a reply,
        the caller will receive `QueueShutDown("Actor exited.")`.
        # Snippet for copying
        ```py
        async def handle_call(
            self,
            msg: Call,
            env: ActorEnv[Call, Cast, Reply],
            reply_sender: TrackedOneShotSender[Reply],
        ) -> None:
            _ = reply_sender.send(...)
        ```
        """
        return

    async def before_exit(
        self,
        run_result: Exception | None,
        _env: ActorEnv[Call, Cast, Reply],
    ) -> Exception | None:
        """Called before the actor exits.
        There are 3 cases when this method is called:
        - The actor task is cancelled. `run_result` is `None`.
        - All message senders are closed / channel is shut down.
            `run_result` is `None`.
        - `init`, `handle_cast`, or `handle_call` returned an exception or
            raised. `run_result` is that exception.

        This method's return value becomes `ActorRunResult.exit_result`.

        # Snippet for copying
        ```py
        async def before_exit(self, run_result: Exception | None, env: ActorEnv[Call, Cast, Reply]) -> Exception | None:
            return run_result
        ```
        """
        return run_result

    async def _handle_call_or_cast(
        self, msg: Msg[Call, Cast, Reply], env: ActorEnv[Call, Cast, Reply]
    ) -> Exception | None:
        match msg:
            case MsgCall(msg=call, reply_sender=reply_sender):
                tracked_sender = env.reply_registry.register(reply_sender)
                return await self.handle_call(call, env, tracked_sender)
            case MsgCast(msg=cast):
                return await self.handle_cast(cast, env)

    async def _handle_continuously(
        self, env: ActorEnv[Call, Cast, Reply]
    ) -> Exception | None:
        while not isinstance(msg := await env.msg_receiver.recv(), QueueShutDown):
            if (err := await self._handle_call_or_cast(msg, env)) is not None:
                return err

    async def _run_till_exit(
        self, env: ActorEnv[Call, Cast, Reply]
    ) -> Exception | None:
        if (err := await self.init(env)) is not None:
            return err
        return await self._handle_continuously(env)

    async def _run_and_handle_exit(
        self, env: ActorEnv[Call, Cast, Reply]
    ) -> Exception | None:
        run_result: Exception | None = None
        try:
            run_result = await self._run_till_exit(env)
        except asyncio.CancelledError:
            pass
        except Exception as err:
            run_result = err
        env.msg_receiver.shutdown(immediate=False)
        try:
            try:
                _reply_shutdown_to_queued_calls(env.msg_receiver)
            except Exception as err:
                if run_result is None:
                    run_result = err
            env.reply_registry.shutdown()
            return await self.before_exit(run_result, env)
        finally:
            env.msg_receiver.shutdown(immediate=True)

    def spawn(
        self,
        channel: MPMC[Msg[Call, Cast, Reply]] | None = None,
        task_group: TaskGroup | None = None,
    ) -> ActorRef[Call, Cast, Reply]:
        """Spawn the actor in an asyncio task.

        `channel` can be:
        - `None`: create an unbounded `MPMC`
        - `MPMC`: reuse an existing channel
        """
        match channel:
            case None:
                msg_sender, msg_receiver = mpmc_channel()
            case MPMC(sender=sender, receiver=receiver):
                msg_sender, msg_receiver = sender, receiver
        actor_ref = ActorRef[Call, Cast, Reply](msg_sender)
        env: ActorEnv[Call, Cast, Reply] = Env(actor_ref, msg_receiver)

        async def _runner() -> ActorRunResult[Call, Cast, Reply]:
            exit_result = await self._run_and_handle_exit(env)
            return ActorRunResult(actor=self, env=env, exit_result=exit_result)

        actor_ref.actor_task = (
            asyncio.create_task(_runner())
            if task_group is None
            else task_group.create_task(_runner())
        )
        return actor_ref


_DOC_PATH = Path(__file__).with_name("actor_doc.md")
try:
    _DOC = _DOC_PATH.read_text(encoding="utf-8")
    __doc__ = _DOC
    Actor.__doc__ = _DOC
except OSError:
    pass
