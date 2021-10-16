from __future__ import annotations


class Parser:
    @classmethod
    def to_percent(cls, values: float | int | list[float | int],
                   totals: float | int | list[float | int]) -> float | int | list[float | int]:
        """
        Método para conversão de valores para porcentagem.

        :param values: Valor ou valores para serem convertidos.
        :param totals: Valor ou valores do total.
        :return: Valor ou valores convertidos para porcentagem.
        """

        if type(values) in [int, float]:
            return (values / totals) * 100
        elif type(values) == list:
            return [(value / total) * 100 for value, total, in zip(values, totals)]
        else:
            print("Campos values e totals devem ser int, float ou list!")
            return -1

    @classmethod
    def to_percent_string(cls, value: float | int, total: float | int) -> str:
        """
        Método para conversão de valores para porcentagem.

        :param value: Valor ou valores para serem convertidos.
        :param total: Valor ou valores do total.
        :return: String to valor em porcento.
        """

        return f'{(value / total) * 100:.2f}%'
