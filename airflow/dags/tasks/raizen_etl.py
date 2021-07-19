#importanto as bibliotecas
import pandas as pd
import numpy as np
from datetime import datetime


def rodar_transformacoes(nome,folhas):
    '''
    Descrição:
        Função que irá rodar o código todo, e o processo de ETL.

    Utilização:
        rodar_transformacoes(nome, folhas)

    Parâmetros:
        nome
            Nome do Arquivo .csv final
        folhas
            Sheets da Planilha em questão
            
    '''
    xls = pd.ExcelFile('/usr/local/airflow/dags/rawdata/vendas-combustiveis-m3.xls')
    for count, value in enumerate(nome):
        extract = ler_tabela(folhas[count], xls)
        transform = transformar(extract)
        load = write_df(transform,nome[count])

def ler_tabela(folha, xls):
    '''
    Descrição:
      Função que irá realizar a leitura da tabela, e completar todos os campos vazios com 0

    Utilização:
      ler_tabela(folha)

    Parâmetros:
        folhas
            Sheets da Planilha em questão
        xls
            Arquivo a ser lido

    Return:
        inicio_df  
            Tabela inicial
        
    '''
    inicio_df = pd.read_excel(xls, folha).fillna(0)
    return inicio_df

def metrica(x):
    '''
    Descrição:
        Função para coletar a métrica (m3) do campo "Product"

    Utilização:
      metrica(x)

    Parâmetros:
      x
        A informação contida no campo "Product"

    Return:
        Retorna a métrica que está entre os parenteses

    '''
    return x[x.find('(')+1:x.find(')')]


def transformar(df): 
    '''
    Descrição:
        Função que vai rodar as principais transformações na tabela

    Utilização:
        transformar(df)

    Parâmetros:
        df
            Tabela que será utilizada para as transformações

    Return:
        formatado_df    
            Tabela já com todas as transformações
    '''
    rodar_teste(df)
    df = df[['COMBUSTÍVEL', 'ANO', 'ESTADO', 'Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']]    
    df.columns = ['product', 'year', 'uf', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    df = df.melt(id_vars=['product', 'year', 'uf'], var_name='MES', value_name='volume')
    df['unit'] = df['product'].apply(metrica)
    df.year = df.year.astype(str)
    df['year_month'] = df.year + "-" + df.MES + "-01"
    df['year_month'] = pd.to_datetime(df['year_month'])
    df['created_at'] = datetime.now()
    formatado_df = df[['year_month','uf','product','unit','volume','created_at']]
    return formatado_df

def write_df(formatado_df, nome):
    '''
    Descrição:
        Função que irá criar o arquivo .csv 

    Utilização:
        write(formatado_df, nome)

    Parâmetros:
        formatado_df
            Tabela já com todas as transformações e formatações
        nome
            Nome do arquivo .csv final
    '''
    formatado_df.sort_values(by=['uf','product'])
    filename = '%s.csv' % nome
    print(filename)
    formatado_df.to_csv("/usr/local/airflow/dags/output/" + filename, header=True, index=False)
    return formatado_df

def rodar_teste(df):
    '''
    Descrição:
        Função que irá realizar a checagem dos dados iniciais da tabela, e ver se a soma total dos meses,
        bate com o campo "total" da tabela

    Utilização:
         rodar_teste(df1)

    Parâmetros:
        df
            Tabela que iremos pegar a soma total dos meses
    '''
    result_1 = df.iloc[0,4:16].sum()
    result_2 = df.iloc[0,3]
    if result_1 != result_2:
        print("Os dados estão divergentes!")
    else:
        print("Os dados estão corretos!")


rodar_transformacoes(["derivados","diesel"],[1,2])



