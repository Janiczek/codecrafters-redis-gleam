import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import glisten
import resp.{type RESP}

pub fn main() -> Nil {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, _state, conn) {
      case msg {
        glisten.Packet(packet) -> {
          io.println("-----------------------")
          let #(cmd, _rest) = resp.parse(packet)
          io.println("REQUEST")
          io.debug(cmd)
          let response = handle(cmd)
          io.println("RESPONSE")
          io.debug(response)
          let response_bb = resp.encode(response)
          let assert Ok(_) = glisten.send(conn, response_bb)
          actor.continue(Nil)
        }
        glisten.User(_) ->
          panic as "Shouldn't have gotten a glisten.User() message"
      }
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}

// HANDLE

fn handle(cmd: RESP) -> RESP {
  case cmd {
    resp.Arr(_, arr) ->
      case arr {
        [] -> panic as "Got empty array as a command"
        [resp.BulkStr(_, "PING")] -> handle_ping()
        [resp.BulkStr(_, "ECHO"), resp.BulkStr(length, echo_)] ->
          handle_echo(length, echo_)
        _ -> panic as "TODO handle"
      }
    resp.SimpleStr(_) -> panic as "Shouldn't have gotten a simple string here"
    resp.BulkStr(_, _) -> panic as "Shouldn't have gotten a bulk string here"
  }
}

fn handle_ping() {
  resp.SimpleStr("PONG")
}

fn handle_echo(length: Int, echo_: String) {
  resp.BulkStr(length, echo_)
}
