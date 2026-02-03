"""
Track Platform - Utilities compartilhadas

O módulo expunha funções que ainda não foram implementadas
(schema_validator, config_parser, incremental, naming) e a importação
falhava imediatamente com ModuleNotFoundError. Para manter o pacote
utilizável enquanto as implementações reais não chegam, definimos
stubs leves que deixam claro que a funcionalidade é pendente sem
quebrar o import do pacote.
"""

# Stubs mínimos para evitar falhas de import; substitua quando os
# utilitários forem implementados de fato.
def validate_schema(*_args, **_kwargs):
    raise NotImplementedError("validate_schema ainda não implementado")


def parse_config(*_args, **_kwargs):
    raise NotImplementedError("parse_config ainda não implementado")


def get_incremental_value(*_args, **_kwargs):
    raise NotImplementedError("get_incremental_value ainda não implementado")


def format_table_name(*_args, **_kwargs):
    raise NotImplementedError("format_table_name ainda não implementado")


__all__ = [
    "validate_schema",
    "parse_config",
    "get_incremental_value",
    "format_table_name",
]
