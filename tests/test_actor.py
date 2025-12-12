import asyncio
import unittest
from asyncio import QueueShutDown

from aio_sync.mpmc import mpmc_channel
from py_gen_server import Actor, spawn


class Recorder(Actor[str, str, str]):
    def __init__(self) -> None:
        super().__init__()
        self.log: list[str] = []

    async def init(self) -> None:
        self.log.append("init")

    async def handle_call(self, msg: str) -> str | None:
        self.log.append(f"call:{msg}")
        return msg.upper()

    async def handle_cast(self, msg: str) -> str | None:
        self.log.append(f"cast:{msg}")
        return None

    async def handle_info(self, msg: object) -> str | None:
        self.log.append(f"info:{msg}")
        return None

    async def terminate(self) -> None:
        self.log.append("term")


class Echo(Actor[int, int, int]):
    async def handle_call(self, msg: int) -> int | None:
        return msg

    async def handle_cast(self, msg: int) -> int | None:
        return None


class ActorTests(unittest.IsolatedAsyncioTestCase):
    async def test_call_and_cast_flow(self) -> None:
        actor = Recorder()
        ref, _ = spawn(actor)
        reply = await ref.call("ping")
        self.assertEqual(reply, "PING")
        await ref.cast("tick")
        await asyncio.sleep(0)
        ref.cancel()
        await ref.wait()
        self.assertEqual(actor.log, ["init", "call:ping", "cast:tick", "term"])

    async def test_call_after_shutdown_reports_queue(self) -> None:
        actor = Echo()
        ref, _ = spawn(actor)
        ref.cancel()
        await ref.wait()
        result = await ref.call(1)
        self.assertIsInstance(result, QueueShutDown)

    async def test_spawn_with_task_group_and_channel(self) -> None:
        sender, receiver = mpmc_channel()
        actor = Recorder()
        async with asyncio.TaskGroup() as tg:
            ref, task = spawn(actor, sender, receiver, task_group=tg)
            await ref.cast("tick")
            await asyncio.sleep(0)
            ref.cancel()
        self.assertTrue(task.done())
        self.assertFalse(ref.is_alive())
        self.assertIn("cast:tick", actor.log)


if __name__ == "__main__":
    unittest.main()
