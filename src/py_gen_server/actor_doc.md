<!-- DO NOT modify manually! Keep this file very close to tokio_gen_server/src/actor_doc.md. -->
# An Elixir/Erlang-GenServer-like actor

Define 3 message types and at least one callback handler on your class to make it an actor.

A GenServer-like actor simply receives messages and acts upon them.
A message is either a "call" (request-reply) or a "cast" (fire-and-forget).
Upon a "call" message, we call `Actor.handle_call`;
upon a "cast" message, we call `Actor.handle_cast`.
Upon cancellation or error, we call `Actor.before_exit`,
so you can gracefully shut down.

## Usage

1. Define your actor class that stores your states and implement `Actor`.
1. Declare your message types.
   - If your actor does not expect any "cast", set `Cast` to `object`.
   - If your actor does not expect any "call", set both `Call` and `Reply` to `object`.
   > Tip: use your editor to automatically generate "required fields".
1. Implement `handle_call` and/or `handle_cast`.
   > Tip: use your editor to automatically generate "provided implementations",
   > then hover on the methods you need and copy the snippets in their docstrings.
1. Implement `init` and `before_exit` if needed.
1. Spawn your actor with `Actor.spawn` (or module-level `spawn`) and get `(handle, actor_ref)`.
1. Use `ActorRef` to send messages to your actor.

## Example

```py
import asyncio
from dataclasses import dataclass
from enum import Enum, auto

from py_gen_server import Actor, ActorEnv


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

    async def init(self, _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount]) -> Exception | None:
        print("PingPongServer starting.")
        return None

    async def handle_cast(
        self,
        msg: PingOrBang,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
    ) -> Exception | None:
        if msg is PingOrBang.Bang:
            return ValueError("Received Bang! Blowing up.")
        self.counter += 1
        print(f"Received ping #{self.counter}")
        return None

    async def handle_call(
        self,
        msg: PingOrPong,
        _env: ActorEnv[PingOrPong, PingOrBang, PongOrCount],
        reply_sender,
    ) -> Exception | None:
        match msg:
            case PingOrPong.Ping:
                self.counter += 1
                print(f"Received ping #{self.counter} as a call")
                reply_sender.send("pong")
            case PingOrPong.Pong:
                reply_sender.send(Count(self.counter))
        return None


async def main() -> None:
    handle, server_ref = PingPongServer().spawn()
    _ = await server_ref.cast(PingOrBang.Ping)
    pong = await server_ref.call(PingOrPong.Ping)
    assert pong == "pong"
    count = await server_ref.call(PingOrPong.Pong)
    assert count == Count(2)
    server_ref.cancel()
    async with asyncio.timeout(0.1):
        rr = await handle
    assert rr.exit_result is None


asyncio.run(main())
```
