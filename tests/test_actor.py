import asyncio
import unittest

from py_gen_server import Actor, HandleResult


# TODO: demonstrate custom union types as message types instead of str
# use `match`
class Recorder(Actor[str, str, str]):
    def __init__(self) -> None:
        super().__init__()
        self.log: list[str] = []

    async def handle_call(self, msg: str) -> HandleResult[str]:
        self.log.append(msg)
        if msg == "stop":
            return HandleResult.stop("bye")
        return HandleResult.cont(msg)

    async def handle_cast(self, msg: str) -> HandleResult[str]:
        self.log.append(msg)
        return HandleResult.cont()

    async def before_exit(self) -> None:
        self.log.append("exit")


class Counter(Actor[str, str, int]):
    def __init__(self) -> None:
        super().__init__()
        self.n = 0

    async def handle_call(self, msg: str) -> HandleResult[int]:
        if msg == "get":
            return HandleResult.cont(self.n)
        return HandleResult.stop(self.n)

    async def handle_cast(self, msg: str) -> HandleResult[int]:
        if msg == "inc":
            self.n += 1
            return HandleResult.cont(self.n)
        return HandleResult.stop(self.n)

    async def before_exit(self) -> None:
        self.n += 100


class Cancelled(Actor[int, int, int]):
    def __init__(self) -> None:
        super().__init__()
        self.cleaned = False

    async def handle_call(self, msg: int) -> HandleResult[int]:
        await asyncio.sleep(0)
        return HandleResult.cont(msg)

    async def handle_cast(self, msg: int) -> HandleResult[int]:
        await asyncio.sleep(0)
        return HandleResult.cont(msg)

    async def before_exit(self) -> None:
        self.cleaned = True


class ActorTests(unittest.IsolatedAsyncioTestCase):
    async def test_call_and_stop_message(self) -> None:
        ref, join = Recorder.spawn()
        reply, err = await ref.call("hello")
        self.assertIsNone(err)
        self.assertEqual(reply, "hello")
        reply, err = await ref.call("stop")
        self.assertIsNone(err)
        self.assertEqual(reply, "bye")
        await join.wait()
        self.assertEqual(join.actor.log, ["hello", "stop", "exit"])

    async def test_cast_and_state(self) -> None:
        ref, join = Counter.spawn()
        self.assertIsNone(await ref.cast("inc"))
        self.assertIsNone(await ref.cast("inc"))
        reply, err = await ref.call("get")
        self.assertIsNone(err)
        self.assertEqual(reply, 2)
        reply, err = await ref.call("halt")
        self.assertIsNone(err)
        self.assertEqual(reply, 2)
        await join.wait()
        self.assertEqual(join.actor.n, 102)

    async def test_stop_cancels_and_cleans(self) -> None:
        ref, join = Cancelled.spawn()
        self.assertIsNone(await ref.cast(1))
        await asyncio.sleep(0)
        await join.cancel()
        self.assertTrue(join.actor.cleaned)


if __name__ == "__main__":
    unittest.main()
