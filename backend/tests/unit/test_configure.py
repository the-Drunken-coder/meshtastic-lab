from types import SimpleNamespace

import pytest
from meshtastic.protobuf import channel_pb2, config_pb2

import backend.app.runtime.configure as configure_module
from backend.app.models import default_scenario


def test_configuration_mismatch_redacts_channel_key_material() -> None:
    scenario = default_scenario(2)
    node = scenario.nodes[0]
    lora = config_pb2.Config.LoRaConfig(
        use_preset=True,
        region=config_pb2.Config.LoRaConfig.RegionCode.Value(scenario.rf.region),
        modem_preset=config_pb2.Config.LoRaConfig.ModemPreset.Value(
            scenario.rf.modem_preset
        ),
        channel_num=scenario.rf.frequency_slot,
        hop_limit=scenario.rf.hop_limit,
    )
    device = config_pb2.Config.DeviceConfig(
        role=config_pb2.Config.DeviceConfig.Role.Value(node.role.value)
    )
    primary = channel_pb2.Channel(role=channel_pb2.Channel.Role.PRIMARY)
    primary.settings.name = scenario.channel.name
    primary.settings.psk = b"secret actual key"
    interface = SimpleNamespace(
        myInfo=SimpleNamespace(my_node_num=1),
        metadata=SimpleNamespace(firmware_version="test"),
        localNode=SimpleNamespace(
            localConfig=SimpleNamespace(lora=lora, device=device),
            channels=[primary],
        ),
        nodesByNum={
            1: {
                "user": {
                    "longName": node.display_name,
                    "shortName": "N1",
                }
            }
        },
    )

    with pytest.raises(configure_module.NodeConfigurationError) as raised:
        configure_module._verify(
            interface,
            node=node,
            rf=scenario.rf,
            channel=scenario.channel,
        )

    detail = str(raised.value)
    assert "channel_psk" in detail
    assert detail.count("<redacted>") == 2
    assert "secret actual key" not in detail
    assert "\\x01" not in detail
