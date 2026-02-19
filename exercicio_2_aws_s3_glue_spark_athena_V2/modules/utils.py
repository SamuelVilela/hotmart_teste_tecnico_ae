import sys
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta

def get_dates_to_process(args):
    # Tenta pegar as datas dos argumentos, se não existir, usa o dia anterior (D-1)
    start_str = args.get('START_DATE')
    end_str = args.get('END_DATE')
    
    if not start_str or not end_str:
        print("### [AVISO] Parâmetros de data não encontrados. Usando D-1 por padrão.")
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_str = start_str or yesterday
        end_str = end_str or yesterday

    # Converte strings para objetos datetime
    start_date = datetime.strptime(start_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_str, '%Y-%m-%d')
    
    delta = end_date - start_date
    dates = []
    
    # Gera a lista de todas as datas no intervalo
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        dates.append(day)
        
    return dates