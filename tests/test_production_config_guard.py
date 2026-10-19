"""Production must not start with development-default or weak secrets; development stays unaffected."""

import pytest

from app.core.config import Settings

STRONG = "k9Qx2mV7pLr4Zt8WyB3nHd6Fs1JcUe5A"
GOOD = dict(ENVIRONMENT="production", JWT_SECRET_KEY=STRONG, JWT_ISSUER="https://auth.example.com", DEBUG=False)
BAD = ["supersecretjwtkeyforauthservicelocaldvelopment12345", "short", ""]


def build(**over):
    return Settings(_env_file=None, **over)


def test_development_defaults_still_start():
    build()                                           # no ENVIRONMENT set: local development


def test_a_strong_production_configuration_starts():
    build(**GOOD)


@pytest.mark.parametrize("secret", BAD)
def test_production_refuses_default_weak_or_missing_jwt_secrets(secret):
    with pytest.raises(ValueError) as exc:
        build(**{**GOOD, "JWT_SECRET_KEY": secret})
    assert "JWT_SECRET_KEY" in str(exc.value)
    assert secret not in str(exc.value) or secret == ""            # the secret value is never echoed


def test_the_environment_name_is_case_insensitive():
    with pytest.raises(ValueError):
        build(**{**GOOD, "ENVIRONMENT": " Production ", "JWT_SECRET_KEY": "short"})


def test_production_requires_the_issuer_and_no_debug():
    with pytest.raises(ValueError) as exc:
        build(**{**GOOD, "JWT_ISSUER": None, "DEBUG": True})
    assert "JWT_ISSUER" in str(exc.value) and "DEBUG" in str(exc.value)
