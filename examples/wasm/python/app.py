import json

from componentize_py_types import Err
from wit_world import exports
from wit_world.imports.types import Header, Request, Response

_signature: str | None = None


class Lifecycle(exports.Lifecycle):
    def initialize(self, config_json: str) -> None:
        global _signature

        try:
            config = json.loads(config_json)
            signature = config["signature"]
            if not isinstance(signature, str):
                raise TypeError("signature must be a string")
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise Err(f"invalid plugin configuration: {error}") from error

        if _signature is not None:
            raise Err("plugin is already initialized")
        _signature = signature


class HttpTransform(exports.HttpTransform):
    def decode_request(self, value: Request) -> Request:
        return value

    def encode_request(self, value: Request) -> Request:
        if _signature is None:
            raise Err("plugin is not initialized")
        _set_header(value, "x-wasm-signature", _signature)
        return value

    def decode_response(self, value: Response) -> Response:
        return value

    def encode_response(self, value: Response) -> Response:
        return value


class HttpRedact(exports.HttpRedact):
    def redact_request(self, value: Request) -> Request:
        _set_header(value, "x-wasm-signature", "<redacted>")
        return value


def _set_header(request: Request, name: str, value: str) -> None:
    for header in request.headers:
        if header.name == name:
            header.value = value
            return
    request.headers.append(Header(name=name, value=value))
