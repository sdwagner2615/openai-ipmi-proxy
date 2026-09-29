"""Config loading, ${ENV} interpolation, and the validation rules."""

import pytest

from relay.config import ConfigError, load_config
from relay.models import NoopPowerConfig, RedfishPowerConfig

pytestmark = pytest.mark.unit


def write_config(tmp_path, text):
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return path


VALID = """
proxy:
  port: 8000
servers:
  - name: devbox
    type: noop
    power:
      initial_state: on
    service_url: http://127.0.0.1:8100
endpoints:
  - name: llm
    server: devbox
    path_prefix: /v1
    catch_all: true
    session_id_body_fields: [user]
"""


def test_loads_a_minimal_valid_config(tmp_path):
    config = load_config(write_config(tmp_path, VALID))
    assert config.proxy.port == 8000
    assert config.servers[0].type == "noop"
    assert isinstance(config.servers[0].power, NoopPowerConfig)
    assert config.endpoints[0].catch_all is True
    assert config.endpoints[0].session_id_body_fields == ["user"]


def test_env_interpolation_resolves_from_process_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_SECRET", "s3cret")
    text = VALID.replace("type: noop", "type: redfish").replace(
        """    power:
      initial_state: on""",
        """    power:
      host: 127.0.0.1
      user: u
      password: ${TEST_SECRET}""",
    )
    config = load_config(write_config(tmp_path, text))
    power = config.servers[0].power
    assert isinstance(power, RedfishPowerConfig)
    assert power.password == "s3cret"


def test_missing_env_var_names_var_and_key(tmp_path, monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    text = VALID.replace(
        """    power:
      initial_state: on""",
        """    power:
      initial_state: ${MISSING_VAR}""",
    )
    with pytest.raises(ConfigError, match="MISSING_VAR"):
        load_config(write_config(tmp_path, text))


def test_env_files_next_to_config_are_loaded(tmp_path, monkeypatch):
    monkeypatch.delenv("FILE_ONLY_VAR", raising=False)
    (tmp_path / "secrets.env").write_text("FILE_ONLY_VAR=from-file\n")
    text = VALID.replace(
        "catch_all: true", "catch_all: true\n    readiness:\n      path: ${FILE_ONLY_VAR}"
    )
    config = load_config(write_config(tmp_path, text))
    assert config.endpoints[0].readiness.path == "/from-file"  # leading slash normalized


def test_process_env_wins_over_env_files(tmp_path, monkeypatch):
    monkeypatch.setenv("SHIELDED_VAR", "from-process")
    (tmp_path / ".env").write_text("SHIELDED_VAR=from-file\n")
    text = VALID.replace(
        "path_prefix: /v1", "path_prefix: /v1\n    readiness:\n      path: ${SHIELDED_VAR}"
    )
    config = load_config(write_config(tmp_path, text))
    assert config.endpoints[0].readiness.path == "/from-process"


def test_duplicate_server_names_rejected(tmp_path):
    text = VALID.replace("name: devbox", "name: a").replace("server: devbox", "server: a")
    text = text.replace(
        "endpoints:",
        """  - name: a
    type: noop
    power:
      initial_state: on
    service_url: http://127.0.0.1:8101
endpoints:""",
    )
    with pytest.raises(ConfigError, match="duplicate server name"):
        load_config(write_config(tmp_path, text))


def test_endpoint_must_reference_configured_server(tmp_path):
    with pytest.raises(ConfigError, match="unknown server"):
        load_config(write_config(tmp_path, VALID.replace("server: devbox", "server: ghost")))


def test_identical_prefix_rejected_overlapping_allowed(tmp_path):
    anchor = "endpoints:\n  - name: llm"
    dup = VALID.replace(
        anchor,
        "endpoints:\n  - name: second\n    server: devbox\n    path_prefix: /v1\n  - name: llm",
    )
    with pytest.raises(ConfigError, match="identical path_prefix"):
        load_config(write_config(tmp_path, dup))
    overlap = VALID.replace(
        anchor,
        "endpoints:\n  - name: specific\n    server: devbox\n    path_prefix: /v1/messages\n"
        "    session_id_body_fields: [metadata.user_id]\n  - name: llm",
    )
    config = load_config(write_config(tmp_path, overlap))
    assert {e.path_prefix for e in config.endpoints} == {"/v1", "/v1/messages"}


def test_second_catch_all_rejected(tmp_path):
    anchor = "endpoints:\n  - name: llm"
    text = VALID.replace(
        anchor,
        "endpoints:\n  - name: other\n    server: devbox\n    path_prefix: /v2\n"
        "    catch_all: true\n  - name: llm",
    )
    with pytest.raises(ConfigError, match="catch_all"):
        load_config(write_config(tmp_path, text))


def test_prefix_must_start_with_slash(tmp_path):
    with pytest.raises(ConfigError, match="must start with '/'"):
        load_config(write_config(tmp_path, VALID.replace("path_prefix: /v1", "path_prefix: v1")))


def test_queued_requires_concurrency(tmp_path):
    text = VALID.replace(
        "path_prefix: /v1", "path_prefix: /v1\n    routing: queued\n    concurrency: 0"
    )
    with pytest.raises(ConfigError, match="concurrency >= 1"):
        load_config(write_config(tmp_path, text))


def test_unknown_enum_values_rejected(tmp_path):
    with pytest.raises(ConfigError, match="one of"):
        load_config(write_config(tmp_path, VALID.replace("type: noop", "type: vmware")))


def test_schedule_must_be_valid_5_field_cron(tmp_path):
    with pytest.raises(ConfigError, match="cron"):
        load_config(
            write_config(
                tmp_path,
                VALID.replace(
                    "service_url: http://127.0.0.1:8100",
                    "service_url: http://127.0.0.1:8100\n    schedule: every-day",
                ),
            )
        )
    config = load_config(
        write_config(
            tmp_path,
            VALID.replace(
                "service_url: http://127.0.0.1:8100",
                "service_url: http://127.0.0.1:8100\n    schedule: '0 2 * * *'",
            ),
        )
    )
    assert config.servers[0].schedule == "0 2 * * *"


def test_redfish_power_requires_host_and_creds(tmp_path):
    text = VALID.replace(
        """    power:
      initial_state: on""",
        """    type: redfish
    power:
      user: u""",
    ).replace("type: noop", "")
    with pytest.raises(ConfigError, match="requires 'host'"):
        load_config(write_config(tmp_path, text))
    missing_user = VALID.replace(
        """    power:
      initial_state: on""",
        """    power:
      host: 127.0.0.1
      password: p""",
    ).replace("type: noop", "type: redfish")
    with pytest.raises(ConfigError, match="'user'"):
        load_config(write_config(tmp_path, missing_user))


def test_redfish_base_url_is_optional_and_recorded(tmp_path):
    text = VALID.replace(
        """    power:
      initial_state: on""",
        """    power:
      host: 127.0.0.1
      user: u
      password: p
      base_url: http://127.0.0.1:8102""",
    ).replace("type: noop", "type: redfish")
    config = load_config(write_config(tmp_path, text))
    assert config.servers[0].power.base_url == "http://127.0.0.1:8102"


def test_aws_ec2_power_needs_keys_or_profile(tmp_path):
    base = VALID.replace(
        """    power:
      initial_state: on""",
        """    power:
      region: us-east-1
      instance_id: i-0""",
    ).replace("type: noop", "type: aws-ec2")
    with pytest.raises(ConfigError, match="access_key\\+secret_key or a profile"):
        load_config(write_config(tmp_path, base))
    with_profile = base.replace("instance_id: i-0", "instance_id: i-0\n      profile: my-profile")
    config = load_config(write_config(tmp_path, with_profile))
    assert config.servers[0].power.profile == "my-profile"


def test_store_path_must_be_creatable(tmp_path):
    # The parent "directory" is an existing file -> not creatable (rule 10).
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    text = VALID.replace("proxy:", f"proxy:\n  store:\n    path: {blocker / 'relay.db'}")
    with pytest.raises(ConfigError, match="not creatable"):
        load_config(write_config(tmp_path, text))


def test_noop_initial_state_accepts_yaml_booleans(tmp_path):
    # Unquoted on/off parse as YAML 1.1 booleans.
    on = load_config(write_config(tmp_path, VALID)).servers[0].power
    assert on.initial_state == "on"
    off = (
        load_config(
            write_config(tmp_path, VALID.replace("initial_state: on", "initial_state: 'off'"))
        )
        .servers[0]
        .power
    )
    assert off.initial_state == "off"
    with pytest.raises(ConfigError, match="initial_state"):
        load_config(
            write_config(tmp_path, VALID.replace("initial_state: on", "initial_state: maybe"))
        )


def test_empty_optional_strings_allowed(tmp_path, monkeypatch):
    monkeypatch.setenv("EMPTY_PW", "")
    text = VALID.replace(
        "endpoints:",
        """clients:
  - name: oc
    status:
      password: ${EMPTY_PW}
endpoints:""",
    )
    config = load_config(write_config(tmp_path, text))
    assert config.clients[0].status.password == ""


def test_config_file_missing_is_an_error(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml")
