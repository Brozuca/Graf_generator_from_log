from datetime import datetime, timedelta
import gzip
from optparse import Values
import sys
import re
from unittest.mock import patch
import matplotlib
from matplotlib import pyplot as plt
from matplotlib import dates
from numpy import empty

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def find_median(array_val):
   values = sorted(set(array_val.values()))
   length = len(values)
   if(length / 2 == 0):
       return (values[length//2 - 1] + values[length//2]) // 2
   else:
       return int(values[length//2])

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

def createGraphMultiplyLines(file_name, title, x_title, y_tilte, data):
    _x = {}
    _y = {}

    for line in data:
        _x[line]=[x for x in data[line]]
        _y[line]=[data[line][x] for x in data[line]]

    fig, ax = plt.subplots()
    plt.style.use('seaborn-colorblind')
    fig.set_figheight(10)
    fig.set_figwidth(16.5)
    plt.title(title, fontsize=16)
    for line in data:
        ax.plot(_x[line], _y[line], label = line )
    ax.set_xlabel(x_title, fontsize=10)
    ax.set_ylabel(y_tilte, fontsize=10)


    ax.grid(axis='both', alpha=.2)
    ax.legend()


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

def difrent_time_in_minuts(t1, t2):
    tdelta = t2 - t1
    return tdelta.total_seconds()

def get_median_and_retun_data(temp_data, data_for_requests):
    result = {}

    median = find_median(temp_data)

    for requset in temp_data:
        if temp_data[requset] >= median:
            result[requset] = data_for_requests[requset]

    return result

def edit_request_name(request):
    path = [word[0].upper() + word[1:] for word in request.split("/")[1:]]
    
    return "".join(path)

def main():

    #В командной строке передаются первый параметр  - файл архива.

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
    data_for_most_frequent = {}
    data_for_most_long={}
    data_for_weighty={}

    #получение кол-ва строк
    with gzip.open(sys.argv[1], mode="rt") as decoder:
        lines_temp = decoder.readlines()
        if lines_temp:
            first_data = parse_data(pattern, lines_temp[1])

            last_data = parse_data(pattern, lines_temp[-1])
        lines = len(lines_temp)

    
        date_generated = [x for x in datetime_range(first_data, last_data + timedelta(seconds=1), timedelta(seconds=1))]

        data_for_all_requests = generate_empty_datetime_dict(date_generated )
        data_for_sucsses_requests = generate_empty_datetime_dict(date_generated )
        data_for_unsuccessful_requests = generate_empty_datetime_dict(date_generated )
        data_for_most_frequent = generate_empty_datetime_dict(date_generated )
        data_for_most_long = generate_empty_datetime_dict(date_generated )
        data_for_most_weighty = generate_empty_datetime_dict(date_generated )


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
                data_for_requests[match.group(4)][datetime_object] += 1
            
            if match.group(6) == "200":
                data_for_sucsses_requests[datetime_object] += 1
            
            if match.group(6) == "500":
                data_for_unsuccessful_requests[datetime_object] += 1
            


            
    print("\n")
  
    # Генерация графиков
    # График интенсивности всех запросов
    graph_count = 0
    graph_limit = len(data_for_requests) + 5
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")

    createGraph("График интенсивности всех запросов","График интенсивности всех запросов", "продолжительность теста", "Интестивность", data_for_all_requests)
    data_for_all_requests.clear
    graph_count += 1
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")

    # Графики интенсивности отдельных запросов
    temp_data_most_frequent = {}
    temp_data_most_long = {} 
    temp_data_most_weighty  = {} 
    for requset in data_for_requests:
        #Генирация граика для текущего запроса в цыкле
        createGraph("График интенсивности запроса " + edit_request_name(requset), "График интенсивности запроса " + requset, "продолжительность теста", "Интестивность", data_for_requests[requset])

        # Подсчет данных для генерации графиков с демонстранцией самых частых, длительных и весомых (count*time) операций.
        temp1 = 0
        temp2 = 0
        before_time = list(data_for_requests[requset].keys())[0]

        for time_cout in data_for_requests[requset]:
            temp1 += data_for_requests[requset][time_cout]
            if time_cout != before_time:
                if data_for_requests[requset][time_cout] !=0:
                    if difrent_time_in_minuts(before_time, time_cout) == 1:
                        temp2 += 1
                    before_time = time_cout
        
        temp_data_most_frequent[requset] = temp1
        temp_data_most_long[requset] = temp2
        temp_data_most_weighty[requset] = temp1 * temp2
        
        print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
        graph_count += 1

    data_for_most_frequent = get_median_and_retun_data(temp_data_most_frequent, data_for_requests)
    createGraphMultiplyLines("График интенсивности самых частых запросов", "График интенсивности самых частых запросов", "продолжительность теста", "Интестивность", data_for_most_frequent)
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
    graph_count += 1 
    data_for_most_frequent.clear

    data_for_most_long = get_median_and_retun_data(temp_data_most_long, data_for_requests)                  
    createGraphMultiplyLines("График интенсивности самых длительных запросов", "График интенсивности самых длительных запросов", "продолжительность теста", "Интестивность", data_for_most_long)
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
    graph_count += 1 
    data_for_most_long.clear

    data_for_most_weighty = get_median_and_retun_data(temp_data_most_weighty, data_for_requests)  
    createGraphMultiplyLines("График интенсивности самых весомых запросов", "График интенсивности самых весомых запросов", "продолжительность теста", "Интестивность",  data_for_most_weighty)
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
    graph_count += 1 
    data_for_most_weighty.clear

    createGraph("График интенсивности успешных запросов ","График интенсивности успешных запросов ", "продолжительность теста", "Интестивность", data_for_sucsses_requests)
    data_for_sucsses_requests={}
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
    graph_count += 1

    createGraph("График интенсивности неуспешных запросов","График интенсивности неуспешных запросов", "продолжительность теста", "Интестивность", data_for_unsuccessful_requests)
    data_for_unsuccessful_requests={}
    print('Сгенерированно %d/%d графиков\r' %(graph_count, graph_limit), end="")
    graph_count += 1

    print("\n")


    print("Все готово\n")

if __name__ == "__main__":
    main()