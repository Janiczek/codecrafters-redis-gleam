import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import gleam/int
import gleam/io
import gleam/list
import gleam/string

pub type RESP {
  SimpleStr(String)
  BulkStr(length: Int, str: String)
  Arr(length: Int, arr: List(RESP))
}

// PARSE

pub fn parse(msg: BitArray) -> #(RESP, BitArray) {
  case msg {
    <<"+", rest0:bits>> -> {
      let #(simple_string, rest1) = parse_simple_string(rest0)
      #(SimpleStr(simple_string), rest1)
    }

    <<"*", rest0:bits>> -> {
      let #(length, rest1) = parse_length(rest0)
      let #(array, rest2) = parse_array(length, rest1)
      #(Arr(length, array), rest2)
    }

    <<"$", rest0:bits>> -> {
      let #(length, rest1) = parse_length(rest0)
      let #(bulk_string, rest2) = parse_bulk_string(length, rest1)
      #(BulkStr(length, bulk_string), rest2)
    }

    _ -> {
      io.println("TODO parse")
      panic as "TODO parse"
    }
  }
}

fn parse_simple_string(msg: BitArray) -> #(String, BitArray) {
  parse_simple_string_help(msg, [])
}

fn parse_simple_string_help(
  msg: BitArray,
  acc: List(UtfCodepoint),
) -> #(String, BitArray) {
  case msg {
    <<"\r\n", rest:bits>> -> #(
      acc
        |> list.reverse
        |> string.from_utf_codepoints,
      rest,
    )

    <<char:utf8_codepoint, rest:bits>> -> {
      parse_simple_string_help(rest, [char, ..acc])
    }

    _ -> panic as "non-8bit aligned data - shouldn't be possible"
  }
}

fn parse_length(msg: BitArray) -> #(Int, BitArray) {
  parse_length_go(msg, [])
}

fn parse_length_go(msg: BitArray, acc: List(UtfCodepoint)) -> #(Int, BitArray) {
  case msg {
    <<>> -> panic as "Length without \\r\\n delimiter - shouldn't be possible"

    <<"\r\n", rest:bits>> -> {
      let assert Ok(length) =
        acc
        |> list.reverse
        |> string.from_utf_codepoints
        |> int.parse
      #(length, rest)
    }

    <<char:utf8_codepoint, rest:bits>> -> parse_length_go(rest, [char, ..acc])

    _ -> panic as "non-8bit aligned data - shouldn't be possible"
  }
}

fn parse_bulk_string(length: Int, msg: BitArray) -> #(String, BitArray) {
  case msg {
    <<chars:bytes-size(length), rest1:bits>> -> {
      let assert Ok(string) = bit_array.to_string(chars)
      case rest1 {
        <<"\r\n", rest2:bits>> -> #(string, rest2)
        _ ->
          panic as "bulk string without \\r\\n delimiter - shouldn't be possible"
      }
    }

    _ -> panic as "not enough data in bulk string - shouldn't be possible"
  }
}

fn parse_array(length: Int, msg: BitArray) -> #(List(RESP), BitArray) {
  parse_array_go(length, msg, [])
}

fn parse_array_go(
  length: Int,
  msg: BitArray,
  acc: List(RESP),
) -> #(List(RESP), BitArray) {
  case length {
    0 -> #(list.reverse(acc), msg)
    _ -> {
      let #(msg_, rest) = parse(msg)
      parse_array_go(length - 1, rest, [msg_, ..acc])
    }
  }
}

// ENCODE

pub fn encode(resp: RESP) -> BytesTree {
  case resp {
    SimpleStr(str) -> encode_simple_str(str)
    BulkStr(length, str) -> encode_bulk_str(length, str)
    Arr(length, arr) -> encode_arr(length, arr)
  }
}

fn encode_simple_str(str: String) -> BytesTree {
  bytes_tree.new()
  |> bytes_tree.append_string("+")
  |> bytes_tree.append_string(str)
  |> bytes_tree.append_string("\r\n")
}

fn encode_bulk_str(length: Int, str: String) -> BytesTree {
  bytes_tree.new()
  |> bytes_tree.append_string("$")
  |> bytes_tree.append_string(int.to_string(length))
  |> bytes_tree.append_string("\r\n")
  |> bytes_tree.append_string(str)
  |> bytes_tree.append_string("\r\n")
}

fn encode_arr(length: Int, arr: List(RESP)) -> BytesTree {
  [
    bytes_tree.from_string("$"),
    bytes_tree.from_string(int.to_string(length)),
    bytes_tree.from_string("\r\n"),
    ..list.map(arr, encode)
  ]
  |> bytes_tree.concat()
}
