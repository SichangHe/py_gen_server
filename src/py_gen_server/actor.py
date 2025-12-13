"""An Elixir/Erlang-GenServer-like actor."""

from __future__ import annotations

import asyncio
from asyncio import FIRST_COMPLETED, QueueShutDown, Task, TaskGroup, create_task, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from aio_sync.mpmc import MPMC, MPMCReceiver, MPMCSender, mpmc_channel
from aio_sync.oneshot import OneShot, OneShotSender


@dataclass(slots=True)
class MsgCall[Call, Reply]:
    """A "call" message (request-reply)."""

    msg: Call
    reply_sender: OneShotSender[Reply]


@dataclass(slots=True)
class MsgCast[Cast]:
    """A "cast" message (fire-and-forget)."""

    msg: Cast


type Msg[Call, Cast, Reply] = MsgCall[Call, Reply] | MsgCast[Cast]
"""A message sent to an actor."""


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
        reply_sender, reply_receiver = OneShot[Reply].channel()
        send_err = await self.msg_sender.send(MsgCall(msg, reply_sender))
        if send_err is not None:
            return send_err

        recv_task = create_task(reply_receiver.recv())
        try:
            if self.actor_task is None:
                return await recv_task
            done, _pending = await wait(
                {recv_task, self.actor_task}, return_when=FIRST_COMPLETED
            )
            if recv_task in done:
                return recv_task.result()
            else:
                try:
                    rr = self.actor_task.result()
                except asyncio.CancelledError:
                    return QueueShutDown("Actor exited.")
                if rr.exit_result is not None:
                    raise rr.exit_result
                return QueueShutDown("Actor exited.")
        finally:
            recv_task.cancel()

    async def relay_call(
        self, msg: Call, reply_sender: OneShotSender[Reply]
    ) -> QueueShutDown | None:
        """Call the actor and let it reply via a given one-shot sender.
        Useful for relaying a call from some other caller."""
        return await self.msg_sender.send(MsgCall(msg, reply_sender))

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
        _reply_sender: OneShotSender[Reply],
    ) -> Exception | None:
        """Called when the actor receives a message and needs to reply.

        Implementations should send the reply using `reply_sender`, otherwise the caller
        may hang.
        # Snippet for copying
        ```py
        async def handle_call(
            self,
            msg: Call,
            env: ActorEnv[Call, Cast, Reply],
            reply_sender: OneShotSender[Reply],
        ) -> None:
            reply_sender.send(...)
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
                return await self.handle_call(call, env, reply_sender)
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
