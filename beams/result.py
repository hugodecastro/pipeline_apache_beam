# -*- coding: utf-8 -*-
import apache_beam as beam

result = beam.Pipeline()

class parse_result(beam.DoFn):

    def process(self, record):
        """
        Regiao, Estado, UF, Governador, TotalCasos, TotalObitos
        """
        uf = record[0]
        regiao = record[1]
        estado = record[2]
        governador = record[3]
        total_casos = record[5]
        total_obitos = record[6]
        yield f'{regiao};{estado};{uf};{governador};{total_casos};{total_obitos}'

Resultado = (
    result
    | "Lendo dados processados" >>beam.io.ReadFromText(r"src\resources\processed\processed.txt")
    | "Separar por ;" >> beam.Map(lambda record: record.split(';'))
    | "Agrupando" >> beam.GroupBy(lambda record: record[0])
    | "Juntando listas" >> beam.Map(lambda record: [record[1][0] + record[1][1]][0])
    | "Mapeando estados" >> beam.ParDo(parse_result())
    | "Saida" >> beam.io.WriteToText(file_path_prefix=r"src\resources\final\result",
                                            file_name_suffix='.csv',
                                            header='Regiao;Estado;UF;Governador;TotalCasos;TotalObitos',
                                            shard_name_template='')
)

result.run()