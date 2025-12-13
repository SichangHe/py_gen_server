import asyncio
import unittest
from asyncio import QueueShutDown
from dataclasses import dataclass
from enum import Enum, auto

from py_gen_server import Actor, ActorEnv, Msg


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
        reply_sender,
    ) -> None:
        match msg:
            case PingOrPong.Ping:
                self.counter += 1
                reply_sender.send("pong")
            case PingOrPong.Pong:
                reply_sender.send(Count(self.counter))

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
        return RuntimeError(f"with error `{run_result!r}` and disregarded messages `{messages!r}`, ")


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
        with self.assertRaises(RuntimeError):
            async with asyncio.timeout(0.1):
                _ = await server_ref.call(PingOrPong.Ping)
        server_ref.cancel()
        async with asyncio.timeout(0.1):
            rr = await handle
        self.assertIsInstance(rr.exit_result, RuntimeError)


if __name__ == "__main__":
    unittest.main()
