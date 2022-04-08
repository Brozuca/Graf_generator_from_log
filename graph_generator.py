from datetime import datetime, timedelta
import gzip
import sys
import re
import matplotlib
from matplotlib import pyplot as plt
from matplotlib import dates
from numpy import empty

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def createGraph(file_name, title, x_title, y_tilte, data):
    x=[x for x in data]
    y=[data[x] for x in data]

    fig, ax = plt.subplots()
    plt.style.use('seaborn-colorblind')
    fig.set_figheight(10)
    fig.set_figwidth(16.5)
    plt.title(title, fontsize=16)
    ax.plot(x, y, color="blue")
    ax.set_xlabel(x_title, fontsize=10)
    ax.set_ylabel(y_tilte, fontsize=10)


    ax.grid(axis='both', alpha=.2)


    fig.savefig("graphs/" + "_".join(file_name.split(".")), bbox_inches='tight')

    fig.clf()
    plt.clf()
    plt.close(fig)

def parse_data(pattern, line):
    _match = re.search(pattern, line)
    #05/Feb/2015:17:03:29 +0300
    _mathc2 = re.search("^(\d\d\/.*?\/\d\d\d\d:\d+?:\d+?:\d+?) \+(\d+?)$", _match.group(2))
    _datetime_object = datetime.strptime(_mathc2.group(1), '%d/%b/%Y:%H:%M:%S')
    return _datetime_object

def generate_empty_datetime_dict(datatime_range):
    dict = {}
    
    for i in datatime_range:
        dict[i] = 0

    return dict

def main():
    #В командной строке передаются параметры первый параметр файл архив, второй параметр название выхожного файла.

    #процент выполнения
    i = 1
    #кол-во строк
    lines = 0
    #regexp паттерн
    pattern = '^"([1234567890.]+)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)"$'
    #
    global first_data
    global last_data
    #
    data_for_all_requests = {}
    data_for_requests = {}
    data_for_sucsses_requests = {}
    data_for_unsuccessful_requests= {}
    empty_date_range = {}

    #получение кол-ва строк
    with gzip.open(sys.argv[1], mode="rt") as decoder:
        lines_temp = decoder.readlines()
        if lines_temp:
            first_data = parse_data(pattern, lines_temp[1])

            last_data = parse_data(pattern, lines_temp[-1])
        lines = len(lines_temp)

    
        date_generated = [x for x in datetime_range(first_data, last_data + timedelta(seconds=1), timedelta(seconds=1))]

        empty_date_range= generate_empty_datetime_dict(date_generated )
        data_for_all_requests = generate_empty_datetime_dict(date_generated )
        data_for_sucsses_requests = generate_empty_datetime_dict(date_generated )
        data_for_unsuccessful_requests = generate_empty_datetime_dict(date_generated )
    

    #построчный проход по лог файлу в архиве, извлечение данных и запись в выходной файл
    i = 1
    with gzip.open(sys.argv[1], mode="rt") as decoder:
        for line in decoder:
            #Обновление статуса прогресса парсинга
            progress = round(float(i) / lines * 100)
            print('Обработано страниц %d%%\r'%progress, end="")
            i += 1

            #Извлечнеи данных и запись в файл.
            match = re.search(pattern, line)
            #05/Feb/2015:17:03:29 +0300
            mathc2 = re.search("^(\d\d\/.*?\/\d\d\d\d:\d+?:\d+?:\d+?) \+(\d+?)$", match.group(2))
            datetime_object = datetime.strptime(mathc2.group(1), '%d/%b/%Y:%H:%M:%S')

            data_for_all_requests[datetime_object] += 1

            if match.group(4) in data_for_requests:
                data_for_requests[match.group(4)][datetime_object] += 1
            else:
                data_for_requests[match.group(4)] = generate_empty_datetime_dict(date_generated )
            
            if match.group(6) == "200":
                data_for_sucsses_requests[datetime_object] += 1
            
            if match.group(6) == "500":
                data_for_unsuccessful_requests[datetime_object] += 1

            
    print("\n")
  
    # Генерация графиков
    # График интенсивности всех запросов
    graph_count = 0
    graph_limit = len(data_for_requests) + 2
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")

    createGraph("all_requests","График интенсивности всех запросов", "продолжительность теста", "Интестивность", data_for_all_requests)
    data_for_all_requests={}
    graph_count += 1
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")

    i = 0
    for requset in data_for_requests:
        
        createGraph("requests " + str(i),"График интенсивности запроса " + requset, "продолжительность теста", "Интестивность", data_for_requests[requset])
        i += 1
        print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
        graph_count += 1
    
    data_for_requests={}

    createGraph("sucsses_requests","График интенсивности успешных запросов ", "продолжительность теста", "Интестивность", data_for_sucsses_requests)
    data_for_sucsses_requests={}
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
    graph_count += 1

    createGraph("unsuccessful_requests","График интенсивности неуспешных запросов", "продолжительность теста", "Интестивность", data_for_unsuccessful_requests)
    data_for_unsuccessful_requests={}
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
    graph_count += 1

    print("\n")


    print("Все готово\n")

if __name__ == "__main__":
    main()