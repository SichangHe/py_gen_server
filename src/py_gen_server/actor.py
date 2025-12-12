from __future__ import annotations

import asyncio
from asyncio import QueueShutDown, Task
from dataclasses import dataclass
from typing import Protocol

from aio_sync.mpmc import MPMCReceiver, MPMCSender, mpmc_channel
from aio_sync.oneshot import OneShot


# FIXME: copy all docstings from tokio_gen_server and adjust where needed
@dataclass(slots=True)
class ActorCall[Call, Reply]:
    """Request expecting a reply."""

    msg: Call
    reply_to: OneShot[Reply | Exception | None]


@dataclass(slots=True)
class ActorCast[Cast]:
    """Fire-and-forget message."""

    msg: Cast


type ActorMsg[Call, Cast, Reply] = ActorCall[Call, Reply] | ActorCast[Cast]
"""Message to be delivered to an actor, either call or cast."""


class Actor[Call, Cast, Reply](Protocol):
    """Async actor interface for building Erlang-style generic servers."""

    async def init(self) -> None:
        return None

    async def handle_call(self, msg: Call) -> Reply | None:
        ...

    async def handle_cast(self, msg: Cast) -> Reply | None:
        ...

    async def before_exit(self) -> None:
        return None

    async def _serve(
        self,
        receiver: MPMCReceiver[ActorMsg[Call, Cast, Reply]],
        ref: ActorRef[Call, Cast, Reply],
    ) -> None:
        task = asyncio.current_task()
        ref.task4actor = task
        await self.init()
        try:
            while True:
                try:
                    env = await receiver.recv()
                except QueueShutDown:
                    break
                match env:
                    case ActorCall(msg=msg, reply_to=reply_to):
                        try:
                            reply = await self.handle_call(msg)
                        except Exception as err:
                            reply_to.send(err)
                        else:
                            reply_to.send(reply)
                    case ActorCast(msg=msg):
                        await self.handle_cast(msg)
        except asyncio.CancelledError:
            pass
        finally:
            receiver.shutdown(immediate=True)
            try:
                await self.before_exit()
            finally:
                self.gs_self = None
                ref.task4actor = None


@dataclass(slots=True)
class ActorRef[Call, Cast, Reply]:
    """A reference to an instance of `Actor`, to cast or call messages on it or
    cancel it."""

    msg_sender: MPMCSender[ActorMsg[Call, Cast, Reply]]
    # TODO: Should return something
    task4actor: Task[None]

    async def cast(self, msg: Cast) -> QueueShutDown | None:
        return await self.msg_sender.send(ActorCast(msg))

    async def call(self, msg: Call) -> Reply | QueueShutDown | Exception | None:
        reply_to = OneShot[Reply | Exception | None]()
        err = await self.msg_sender.send(ActorCall(msg, reply_to))
        if isinstance(err, QueueShutDown):
            return err
        wait_for: set[asyncio.Future[Reply | Exception | None] | Task[None]] = {
            reply_to.future
        }
        task = self.task4actor
        if task is not None:
            wait_for.add(task)
        done, _ = await asyncio.wait(wait_for, return_when=asyncio.FIRST_COMPLETED)
        if reply_to.future in done:
            return await reply_to.recv()
        return QueueShutDown()

    async def relay_call(
        self, msg: Call, reply_to: OneShot[Reply | Exception | None]
    ) -> QueueShutDown | None:
        """Forward a call using an existing reply path."""

        err = await self.msg_sender.send(ActorCall(msg, reply_to))
        if isinstance(err, QueueShutDown):
            return err
        return None

    def blocking_call(self, msg: Call) -> Reply | Exception | QueueShutDown | None:
        """Run `call` in a new event loop."""

        return asyncio.run(self.call(msg))

    def cancel(self) -> None:
        """Cancel the actor task without waiting."""

        task = self.task4actor
        if task is None or task.done():
            return None
        task.cancel()

    async def wait(self) -> None:
        """Wait for the actor task to finish."""

        task = self.task4actor
        if task is None:
            return None
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            return None

    def is_alive(self) -> bool:
        """Return True while the actor task is active."""

        task = self.task4actor
        return task is not None and not task.done()


def spawn[Call, Cast, Reply](
    actor: Actor[Call, Cast, Reply],
    /,
    sender: MPMCSender[ActorMsg[Call, Cast, Reply]] | None = None,
    receiver: MPMCReceiver[ActorMsg[Call, Cast, Reply]] | None = None,
    *,
    task_group: asyncio.TaskGroup | None = None,
) -> tuple[ActorRef[Call, Cast, Reply], Task[None]]:
    """Start an actor with explicit channels, mirroring `spawn` in Rust."""

    if (sender is None) != (receiver is None):
        raise ValueError("provide sender and receiver together")
    if sender is None or receiver is None:
        sender, receiver = mpmc_channel()
    ref = ActorRef(sender, actor)
    if task_group is None:
        task = asyncio.create_task(actor._serve(receiver, ref))
    else:
        task = task_group.create_task(actor._serve(receiver, ref))
    ref.task4actor = task
    return ref, task
