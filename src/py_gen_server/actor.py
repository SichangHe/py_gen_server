from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from asyncio import QueueShutDown, Task
from dataclasses import dataclass

from aio_sync.mpmc import MPMCReceiver, MPMCSender, mpmc_channel
from aio_sync.oneshot import OneShot


# FIXME: unnecessary. just let actor return None | Exception
@dataclass(slots=True)
class HandleResult[R]:
    """Handler outcome with optional reply and stop control."""

    reply: R | None = None
    should_stop: bool = False

    @classmethod
    def cont(cls, reply: R | None = None) -> HandleResult[R]:
        return cls(reply=reply, should_stop=False)

    @classmethod
    def stop(cls, reply: R | None = None) -> HandleResult[R]:
        return cls(reply=reply, should_stop=True)


# FIXME: separate to ActorCall, etc. and provide a type alias for union
class ActorMsg:
    # FIXME: Use full name for generic arguments
    @dataclass(slots=True)
    class Call[C, R]:
        msg: C
        reply_to: OneShot[R]

    @dataclass(slots=True)
    class Cast[C]:
        msg: C

    @dataclass(slots=True)
    class Reply[R]:
        msg: R


class Actor[Call, Cast, Reply](ABC):
    """Abstract actor matching the Rust trait surface."""

    # FIXME: should not be inside Actor, but should be in ActorRef
    gs_self_task: Task[None] | None

    def __init__(self) -> None:
        self.gs_self_task = None

    # FIXME: write docstrings w/ examples for every public thing
    async def init(self) -> None:
        return None

    @abstractmethod
    async def handle_call(self, msg: Call) -> HandleResult[Reply]: ...

    @abstractmethod
    async def handle_cast(self, msg: Cast) -> HandleResult[Reply]: ...

    async def before_exit(self) -> None:
        return None

    # FIXME: match signatures of all tokio_gen_server Actor-related trait methods
    async def _run(
        self,
        receiver: MPMCReceiver[
            ActorMsg.Call[Call, Reply] | ActorMsg.Cast[Cast] | ActorMsg.Reply[Reply]
        ],
    ) -> None:
        self.gs_self_task = asyncio.current_task()
        await self.init()
        try:
            while True:
                env = await receiver.recv()
                match env:
                    case QueueShutDown():
                        break
                    case ActorMsg.Call(msg=msg, reply_to=reply_to):
                        result = await self.handle_call(msg)
                        reply_to.send(result.reply)
                        if result.should_stop:
                            break
                    case ActorMsg.Cast(msg=msg):
                        result = await self.handle_cast(msg)
                        if result.should_stop:
                            break
                    case ActorMsg.Reply():
                        continue
        except asyncio.CancelledError:
            pass
        finally:
            receiver.shutdown(immediate=True)
            try:
                await self.before_exit()
            finally:
                self.gs_self_task = None

    # FIXME: do not use a classmethod here! spawn an already constructed actor
    @classmethod
    def spawn(
        cls, *args, **kwargs
    ) -> tuple[ActorRef[Call, Cast, Reply], ActorJoinHandle[Call, Cast, Reply]]:
        actor: Actor[Call, Cast, Reply] = cls(*args, **kwargs)
        sender, receiver = mpmc_channel()
        task = asyncio.create_task(actor._run(receiver))
        return ActorRef(sender), ActorJoinHandle(actor, task)


class ActorRef[Call, Cast, Reply]:
    """User handle to interact w a running actor."""

    sender: MPMCSender[
        ActorMsg.Call[Call, Reply] | ActorMsg.Cast[Cast] | ActorMsg.Reply[Reply]
    ]

    def __init__(
        self,
        sender: MPMCSender[
            ActorMsg.Call[Call, Reply] | ActorMsg.Cast[Cast] | ActorMsg.Reply[Reply]
        ],
    ) -> None:
        self.sender = sender

    async def cast(self, msg: Cast) -> QueueShutDown | None:
        err = await self.sender.send(ActorMsg.Cast(msg))
        if isinstance(err, QueueShutDown):
            return err
        return None

    async def call(self, msg: Call) -> tuple[Reply | None, QueueShutDown | None]:
        reply = OneShot[Reply]()
        err = await self.sender.send(ActorMsg.Call(msg, reply))
        if isinstance(err, QueueShutDown):
            return None, err
        return await reply.recv(), None

    async def relay_call(
        self, msg: Call, reply_to: OneShot[Reply]
    ) -> QueueShutDown | None:
        err = await self.sender.send(ActorMsg.Call(msg, reply_to))
        if isinstance(err, QueueShutDown):
            return err
        return None

    def blocking_cast(self, msg: Cast) -> QueueShutDown | None:
        return asyncio.run(self.cast(msg))

    def blocking_call(self, msg: Call) -> tuple[Reply | None, QueueShutDown | None]:
        return asyncio.run(self.call(msg))

    # FIXME: def cancel


# FIXME: unnecessary, replace with bare `Task`
class ActorJoinHandle[Call, Cast, Reply]:
    """Join handle for observing and controlling a running actor."""

    actor: Actor[Call, Cast, Reply]
    task: Task[None]

    def __init__(self, actor: Actor[Call, Cast, Reply], task: Task[None]) -> None:
        self.actor = actor
        self.task = task

    async def cancel(self) -> None:
        task = self.actor.gs_self_task
        if task is None or task.done():
            return None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return None

    async def wait(self) -> None:
        task = self.actor.gs_self_task or self.task
        if task is None:
            return None
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            return None

    def is_alive(self) -> bool:
        task = self.actor.gs_self_task or self.task
        return task is not None and not task.done()
