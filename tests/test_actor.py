import asyncio
import unittest
from asyncio import QueueShutDown
from dataclasses import dataclass
from enum import Enum, auto

from py_gen_server import Actor, ActorEnv, ActorRef, Msg, TrackedOneShotSender


class PingOrBang(Enum):
    Ping = auto()
    Bang = auto()


class PingOrPong(Enum):
    Ping = auto()
    Pong = auto()


@dataclass(frozen=True, slots=True)
class Count:
    counter: int


type PongOrCount = str | Count


class PingPongServer(Actor[PingOrPong, PingOrBang, PongOrCount]):
    def __init__(self) -> None:
        self.counter = 0

    async def init(self, _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount]) -> None:
        return

    async def handle_cast(
        self,
        msg: PingOrBang,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
    ) -> None:
        if msg is PingOrBang.Bang:
            raise ValueError("Received Bang! Blowing up.")
        self.counter += 1

    async def handle_call(
        self,
        msg: PingOrPong,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
        reply_sender: TrackedOneShotSender[PongOrCount],
    ) -> None:
        match msg:
            case PingOrPong.Ping:
                self.counter += 1
                _ = reply_sender.send("pong")
            case PingOrPong.Pong:
                _ = reply_sender.send(Count(self.counter))

    async def before_exit(
        self,
        run_result: Exception | None,
        env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
    ) -> Exception | None:
        if run_result is None:
            return None
        messages: list[Msg[PingOrPong, PingOrBang, PongOrCount]] = []
        while True:
            try:
                maybe_msg = env.msg_receiver.try_recv()
            except QueueShutDown:
                break
            if maybe_msg is None:
                break
            messages.append(maybe_msg)
        return RuntimeError(
            f"with error `{run_result!r}` and disregarded messages `{messages!r}`, "
        )


class RelayPingPongServer(Actor[PingOrPong, PingOrBang, PongOrCount]):
    def __init__(
        self, downstream_ref: ActorRef[PingOrPong, PingOrBang, PongOrCount]
    ) -> None:
        self.downstream_ref = downstream_ref

    async def init(self, _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount]) -> None:
        return

    async def handle_cast(
        self,
        _msg: PingOrBang,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
    ) -> None:
        return

    async def handle_call(
        self,
        msg: PingOrPong,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
        reply_sender: TrackedOneShotSender[PongOrCount],
    ) -> None:
        relay_err = await self.downstream_ref.relay_call(msg, reply_sender)
        assert relay_err is None


class BlockingPingPongServer(Actor[PingOrPong, PingOrBang, PongOrCount]):
    def __init__(self) -> None:
        self.call_started = asyncio.Event()

    async def init(self, _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount]) -> None:
        return

    async def handle_cast(
        self,
        _msg: PingOrBang,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
    ) -> None:
        return

    async def handle_call(
        self,
        _msg: PingOrPong,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
        _reply_sender: TrackedOneShotSender[PongOrCount],
    ) -> None:
        self.call_started.set()
        await asyncio.Future()


class BlockingCastPingPongServer(Actor[PingOrPong, PingOrBang, PongOrCount]):
    def __init__(self) -> None:
        self.cast_started = asyncio.Event()

    async def init(self, _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount]) -> None:
        return

    async def handle_cast(
        self,
        _msg: PingOrBang,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
    ) -> None:
        self.cast_started.set()
        await asyncio.Future()

    async def handle_call(
        self,
        msg: PingOrPong,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
        reply_sender: TrackedOneShotSender[PongOrCount],
    ) -> None:
        match msg:
            case PingOrPong.Ping:
                _ = reply_sender.send("pong")
            case PingOrPong.Pong:
                _ = reply_sender.send(Count(0))


class ActorTests(unittest.IsolatedAsyncioTestCase):
    async def test_ping_pong(self) -> None:
        server = PingPongServer()
        server_ref = server.spawn()
        assert server_ref.actor_task is not None
        handle = server_ref.actor_task
        _ = await server_ref.cast(PingOrBang.Ping)
        pong = await server_ref.call(PingOrPong.Ping)
        self.assertEqual(pong, "pong")
        count = await server_ref.call(PingOrPong.Pong)
        self.assertEqual(count, Count(2))
        server_ref.cancel()
        async with asyncio.timeout(0.1):
            rr = await handle
        self.assertIsNone(rr.exit_result)

    async def test_ping_pong_bang(self) -> None:
        server = PingPongServer()
        server_ref = server.spawn()
        assert server_ref.actor_task is not None
        handle = server_ref.actor_task
        _ = await server_ref.cast(PingOrBang.Bang)
        async with asyncio.timeout(0.1):
            maybe_shutdown = await server_ref.call(PingOrPong.Ping)
        self.assertIsInstance(maybe_shutdown, QueueShutDown)
        server_ref.cancel()
        async with asyncio.timeout(0.1):
            rr = await handle
        self.assertIsInstance(rr.exit_result, RuntimeError)

    async def test_call_returns_queue_shutdown_on_cancel(self) -> None:
        server = PingPongServer()
        server_ref = server.spawn()
        assert server_ref.actor_task is not None
        handle = server_ref.actor_task
        call_task = asyncio.create_task(server_ref.call(PingOrPong.Ping))
        server_ref.cancel()
        async with asyncio.timeout(0.1):
            maybe_shutdown = await call_task
        self.assertIsInstance(maybe_shutdown, QueueShutDown)
        with self.assertRaises(asyncio.CancelledError):
            async with asyncio.timeout(0.1):
                _ = await handle

    async def test_queued_call_returns_queue_shutdown_on_cancel(self) -> None:
        server = BlockingCastPingPongServer()
        server_ref = server.spawn()
        assert server_ref.actor_task is not None
        handle = server_ref.actor_task

        cast_task = asyncio.create_task(server_ref.cast(PingOrBang.Ping))
        async with asyncio.timeout(0.1):
            await server.cast_started.wait()

        call_task = asyncio.create_task(server_ref.call(PingOrPong.Ping))
        server_ref.cancel()

        async with asyncio.timeout(0.1):
            maybe_shutdown = await call_task
        self.assertIsInstance(maybe_shutdown, QueueShutDown)
        async with asyncio.timeout(0.1):
            cast_err = await cast_task
        self.assertIsNone(cast_err)
        async with asyncio.timeout(0.1):
            rr = await handle
        self.assertIsNone(rr.exit_result)

    async def test_call_after_immediate_cancel_returns_queue_shutdown(self) -> None:
        server = PingPongServer()
        server_ref = server.spawn()
        _ = server_ref.cancel()
        async with asyncio.timeout(0.1):
            maybe_shutdown = await server_ref.call(PingOrPong.Ping)
        self.assertIsInstance(maybe_shutdown, QueueShutDown)

    async def test_actor_to_actor_relay_uses_upstream_registry(self) -> None:
        downstream = PingPongServer()
        downstream_ref = downstream.spawn()
        assert downstream_ref.actor_task is not None
        downstream_handle = downstream_ref.actor_task

        relay = RelayPingPongServer(downstream_ref)
        relay_ref = relay.spawn()
        assert relay_ref.actor_task is not None
        relay_handle = relay_ref.actor_task

        relayed = await relay_ref.call(PingOrPong.Ping)
        self.assertEqual(relayed, "pong")

        relay_ref.cancel()
        downstream_ref.cancel()
        async with asyncio.timeout(0.1):
            relay_result = await relay_handle
        async with asyncio.timeout(0.1):
            downstream_result = await downstream_handle
        self.assertIsNone(relay_result.exit_result)
        self.assertIsNone(downstream_result.exit_result)
        self.assertFalse(relay_result.env.reply_registry.in_flight)
        self.assertFalse(downstream_result.env.reply_registry.in_flight)

    async def test_relayed_call_gets_shutdown_when_downstream_exits_after_dequeue(
        self,
    ) -> None:
        downstream = BlockingPingPongServer()
        downstream_ref = downstream.spawn()
        assert downstream_ref.actor_task is not None
        downstream_handle = downstream_ref.actor_task

        relay = RelayPingPongServer(downstream_ref)
        relay_ref = relay.spawn()
        assert relay_ref.actor_task is not None
        relay_handle = relay_ref.actor_task

        call_task = asyncio.create_task(relay_ref.call(PingOrPong.Ping))
        async with asyncio.timeout(0.1):
            await downstream.call_started.wait()

        downstream_ref.cancel()
        async with asyncio.timeout(0.1):
            maybe_shutdown = await call_task
        self.assertIsInstance(maybe_shutdown, QueueShutDown)

        relay_ref.cancel()
        async with asyncio.timeout(0.1):
            relay_result = await relay_handle
        async with asyncio.timeout(0.1):
            downstream_result = await downstream_handle
        self.assertIsNone(relay_result.exit_result)
        self.assertIsNone(downstream_result.exit_result)


if __name__ == "__main__":
    unittest.main()
