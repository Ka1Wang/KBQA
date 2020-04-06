import json
def read_SPARQL(data_path):
    f = open(data_path, 'r')
    SPARQL = json.load(f) ### SPARQL为dict类型
    #print('type(SPARQL): ', type(SPARQL))
    SPARQL_new = []
    answer = []
    print('Start processing dataset')
    progress_bar = progressbar.ProgressBar(max_value=len(SPARQL['questions']))
    ### ground truth和SPARQLWrapper查询结果在'type'等字段值上有差别，但是在'value'字段值上一定一致，因此我们只比较'value'字段值
    ### 数据的嵌套层数较多，在获取答案值的时候，遵循一个原则：
    ### 如果遇到dict类型，且答案值一定在相应键值下，例如：question->'answers'、answers_temp->'results'->'bindings'、values_temp->'value'
    ### 就直接索引
    ### 如果遇到list类型，例如：question['answers']->、answers_temp['results']['bindings']->，
    ### 或者遇到dict类型，且答案值的索引不唯一，例如：results_temp->'uri'、'string'
    ### 就用for循环遍历
    for i, question in enumerate(SPARQL['questions']):
        #print(i)
        SPARQL_new.append(question['query']['sparql'])
        answer.append([])
        for answers_temp in question['answers']:
            ### 注意：如果查询不到结果，answers_temp中可能不会有键值'results'，answers_temp['results']中可能不会有'bindings'键值
            ### Python的逻辑运算符and和or遵循短路逻辑
            if ('results' in answers_temp) and ('bindings' in answers_temp['results']):
                for results_temp in answers_temp['results']['bindings']:
                    for values_temp in results_temp.values():
                        answer[-1].append(values_temp['value'])

        progress_bar.update(i + 1)
    '''
    ### 测试question['answers']的长度
        if len(question['answers']) != 1:
            print('%dth len(question[\'answers\']) != 1' % (i))
            exit(0)
    print('\n')
    print('ALL len(question[\'answers\']) == 1')
    exit(0)
    '''
    print('\n')
    SPARQL = SPARQL_new

    return SPARQL, answer
import numpy as np
from SPARQLWrapper import SPARQLWrapper, JSON
import progressbar  ### pip install progressbar2
### SPARQL的类型是list->str，形状是(数据量, 每条SPARQL语句的长度)
def execute_SPARQL(endpoint, SPARQL, answer):
    SPARQL_executor = SPARQLWrapper(endpoint)
    SPARQL_executor.setReturnFormat(JSON)
    failure_SPARQL = []
    success_SPARQL = []
    answer_query = []
    answer_query_class = {'system_error': 0, 'query_error': 1, 'correctness': 2}
    evaluate = [] ### 0代表报错；1代表没有报错，且查询结果和ground truth不一致；2代表没有报错，且查询结果和ground truth一致

    print('Start executing SPARQL')
    progress_bar = progressbar.ProgressBar(max_value=len(SPARQL))
    for i, SPARQL_temp in enumerate(SPARQL):
        try:
            ### 需要翻墙。如果不翻墙，很大概率无法连接到数据库，而返回错误 ConnectionResetError: [Errno 104] Connection reset by peer
            SPARQL_executor.setQuery(SPARQL_temp)
            answers_temp = SPARQL_executor.query().convert() ### type(result): <class 'dict'>
        except:
            ### 针对没有翻墙，无法连接到数据库，从而造成的错误 ConnectionResetError: [Errno 104] Connection reset by peer
            failure_SPARQL.append(SPARQL_temp)
            evaluate.append(answer_query_class['system_error'])
        else:
            ### 如果成功
            success_SPARQL.append(SPARQL_temp)
            answer_query.append([])
            ### 注意：如果查询不到结果，answers_temp中可能不会有键值'results'，answers_temp['results']中可能不会有'bindings'键值
            ### Python的逻辑运算符and和or遵循短路逻辑
            if ('results' in answers_temp) and ('bindings' in answers_temp['results']):
                for results_temp in answers_temp['results']['bindings']:
                    for values_temp in results_temp.values():
                        answer_query[-1].append(values_temp['value'])
            if len(answer[i]) != len(answer_query[-1]):
                evaluate.append(answer_query_class['query_error'])
            else:
                ### 若list的元素相同，则排序后，对应位置的元素全部相同；
                ### 逆命题，若排序后，对应位置的元素存在不同，则list的元素不同
                answer_copy = answer[i].copy()
                answer_query_copy = answer_query[-1].copy()
                answer_copy.sort()
                answer_query_copy.sort()
                ### for......else......的执行顺序为：
                ### 当迭代对象完成所有迭代后且此时的迭代对象为空时，
                ### 如果存在else子句则执行else子句，没有则继续执行后续代码；
                ### 如果迭代对象因为某种原因（如带有break关键字）提前退出迭代，
                ### 则else子句不会被执行，程序将会直接跳过else子句继续执行后续代码
                for j in range(len(answer_copy)):
                    if answer_copy[j] != answer_query_copy[j]:
                        evaluate.append(answer_query_class['query_error'])
                        break
                else:
                    evaluate.append(answer_query_class['correctness'])
        np.array(evaluate)
        progress_bar.update(i + 1)
    print('\n')

    print('The number of failure SPARQL: ', len(failure_SPARQL))
    f = open('failure_SPARQL.txt', 'w')
    f.write(str(failure_SPARQL))
    f.close()
    print('The number of success SPARQL: ', len(success_SPARQL))
    f = open('success_SPARQL.txt', 'w')
    f.write(str(success_SPARQL))
    f.close()
    f = open('answer_query.txt', 'w')
    f.write(str(answer_query))
    f.close()
    for class_name in answer_query_class:
        print('%s: %.6f' % (class_name, np.where(np.array(evaluate)==answer_query_class[class_name])[0].shape[0] / len(evaluate)))

if __name__=='__main__':
    data_path = 'qald-9-test-multilingual.json'
    endpoint = 'http://dbpedia.org/sparql'
    
    SPARQL, answer = read_SPARQL(data_path)
    execute_SPARQL(endpoint=endpoint, SPARQL=SPARQL, answer=answer)