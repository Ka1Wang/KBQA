import json
import progressbar
import copy
def read_SPARQL(data_path, do_parsing):
    if do_parsing == True:
        f = open(data_path, 'r')
        data = json.load(f) ### data为list->dict类型
        #print('type(data): ', type(data))
        '''
        ### WebQSP
        new_data = []
        print('Start processing dataset')
        progress_bar = progressbar.ProgressBar(max_value=len(data['Questions']))
        new_data_temp = {}
        for i, Questions_temp in enumerate(data['Questions']):
            for j, Parses_temp in enumerate(Questions_temp['Parses']):
                #print(i)
                ### Parses_temp['Sparql'] 可能会是 null，json的null 对应 python的None
                ### 跳过Parses_temp['Sparql']值为 null 的Question
                if Parses_temp['Sparql'] == None:
                    #print('%s Parses_temp[\'Sparql\']: ' % (Questions_temp['QuestionId']), Parses_temp['Sparql'])
                    continue

                new_data_temp.update({'question': Questions_temp['RawQuestion']})
                new_data_temp.update({'sparql': Parses_temp['Sparql']})
                new_data_temp.update({'PotentialTopicEntityMention': Parses_temp['PotentialTopicEntityMention']})
                new_data_temp.update({'TopicEntityName': Parses_temp['TopicEntityName']})
                new_data_temp.update({'TopicEntityMid': Parses_temp['TopicEntityMid']})
                new_data_temp.update({'InferentialChain': Parses_temp['InferentialChain']})
                new_data_temp.update({'Constraints': Parses_temp['Constraints']})
                new_data.append(copy.deepcopy(new_data_temp))
                ### copy.deepcopy()的目的是保存深复制的版本，否则下一次迭代修改temp dict的字段时，会同时修改已保存的数据
            
            progress_bar.update(i + 1)
        data = copy.deepcopy(new_data)
        '''

        '''
        ### 测试Questions_temp['Parses']的长度
            if len(Questions_temp['Parses']) != 1:
                print('%s len(Questions_temp[\'Parses\']) != 1' % (Questions_temp['QuestionId']))
                exit(0)
        print('\n')
        print('ALL len(Questions_temp) == 1')
        exit(0)
        '''
        '''
        print('\n')
        '''

        ### 保存数据
        f = open('data_in_SPARQL.json', 'w')
        json.dump(obj=data, fp=f, indent=4)
        f.close()

    else:
        ### 读取数据
        f = open('data_in_SPARQL.json', 'r')
        data = json.load(f) ### data为list->dict类型
        f.close()

    return data

import os
def SPARQL_to_JSON(data, do_parsing):
    if do_parsing == True:
        ### 默认SPARQL全部有值
        number_parsable = 0
        number_unparsable = 0
        print('Start transforming SPARQL to JSON')
        progress_bar = progressbar.ProgressBar(max_value=len(data))
        for i in range(len(data)):
            '''
            ### WebQSP
            ### 处理WebQSP的SPARQL缺陷
            SPARQL_temp = copy.deepcopy(data[i]['sparql'])
            ### 无法解析 'OR' 运算符
            ### 已解决， 用 ' || ' 替换 ' OR '
            SPARQL_temp = SPARQL_temp.replace(' OR ', ' || ')
            ### prefix xsd未定义
            ### 已解决，如果SPARQL中有'xsd:'，那么在SPARQL开头加'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n'
            if SPARQL_temp.find('xsd:') != -1:
                SPARQL_temp = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n' + SPARQL_temp
            ### 无法解析 'GROUP BY ?city\nHaving COUNT(?city) = 2'
            ### 已解决 'GROUP BY ?city\nHaving (COUNT(?city) = 2)'
            SPARQL_temp = SPARQL_temp.replace('GROUP BY ?city\nHaving COUNT(?city) = 2', 'GROUP BY ?city\nHaving (COUNT(?city) = 2)')
            '''
            ### QALD
            SPARQL_temp = copy.deepcopy(data[i]['query']['sparql'])
            ### 处理WebQSP的SPARQL缺陷
            ### 无法解析 'SELECT COUNT(DISTINCT ?uri)'
            ### SELECT COUNT(DISTINCT ?uri)
            ### ----------------------^
            ### Expecting '*', 'VAR', '(', 'DISTINCT', 'REDUCED', got 'COUNT'
            ### 未解决
            ### prefix xsd未定义
            ### 已解决，如果SPARQL中有'xsd:'，那么在SPARQL开头加'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n'
            if SPARQL_temp.find('xsd:') != -1:
                SPARQL_temp = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n' + SPARQL_temp
            ### prefix rdf未定义
            ### 已解决，如果SPARQL中有'rdf:'，那么在SPARQL开头加'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n'
            if SPARQL_temp.find('rdf:') != -1:
                SPARQL_temp = 'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n' + SPARQL_temp
            ### prefix foaf未定义
            ### 已解决，如果SPARQL中有'foaf:'，那么在SPARQL开头加'PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n'
            if SPARQL_temp.find('foaf:') != -1:
                SPARQL_temp = 'PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n' + SPARQL_temp
            ### prefix dbo未定义
            ### 已解决，如果SPARQL中有'dbo:'，那么在SPARQL开头加'PREFIX dbo: <http://dbpedia.org/ontology/>\n'
            if SPARQL_temp.find('dbo:') != -1:
                SPARQL_temp = 'PREFIX dbo: <http://dbpedia.org/ontology/>\n' + SPARQL_temp
            ### prefix dbp未定义
            ### 已解决，如果SPARQL中有'dbp:'，那么在SPARQL开头加'PREFIX dbp: <http://dbpedia.org/property/>\n'
            if SPARQL_temp.find('dbp:') != -1:
                SPARQL_temp = 'PREFIX dbp: <http://dbpedia.org/property/>\n' + SPARQL_temp
            ### prefix dbr未定义
            ### 已解决，如果SPARQL中有'dbr:'，那么在SPARQL开头加'PREFIX dbr: <http://dbpedia.org/resource/>\n'
            if SPARQL_temp.find('dbr:') != -1:
                SPARQL_temp = 'PREFIX dbr: <http://dbpedia.org/resource/>\n' + SPARQL_temp    
            ### prefix dct未定义
            ### 已解决，如果SPARQL中有'dct:'，那么在SPARQL开头加'PREFIX dct: <http://purl.org/dc/terms/>\n'
            if SPARQL_temp.find('dct:') != -1:
                SPARQL_temp = 'PREFIX dct: <http://purl.org/dc/terms/>\n' + SPARQL_temp
            ### prefix dbc未定义
            ### 已解决，如果SPARQL中有'dbc:'，那么在SPARQL开头加'PREFIX dbc: <http://dbpedia.org/resource/Category:>\n'
            if SPARQL_temp.find('dbc:') != -1:
                SPARQL_temp = 'PREFIX dbc: <http://dbpedia.org/resource/Category:>\n' + SPARQL_temp

            SPARQL_buffer = open('SPARQL_buffer.sparql', 'w')
            SPARQL_buffer.write(SPARQL_temp)
            SPARQL_buffer.close()

            f = open('mutex.txt', 'w')
            f.write('0')
            f.close()
            os.system('node SPARQL.js-master/bin/SPARQL_to_JSON.js')
            ### 当互斥量为'1'时，说明写入完毕
            while True:
                f = open('mutex.txt', 'r')
                mutex = f.read()
                f.close()
                if mutex == '1':
                    break

            f = open('JSON_buffer.json', 'r')
            JSON = json.load(f)
            data[i].update({'json': copy.deepcopy(JSON)}) ### json.load()返回dict类型
            if JSON != None:
                number_parsable += 1
            else:
                number_unparsable += 1

            progress_bar.update(i + 1)
        print('\n')
        print('The number of parsable SPARQL: ', number_parsable)
        print('The number of unparsable SPARQL: ', number_unparsable)

        ### 保存数据
        f = open('data_in_JSON.json', 'w')
        json.dump(obj=data, fp=f, indent=4)
        f.close()

    else:
        ### 读取数据
        f = open('data_in_JSON.json', 'r')
        data = json.load(f)   
        f.close()

    return data

count = {}
def count_add(s):
    if s not in count:
        count.update({s: 0})
    count[s] += 1

def JSON_to_QueryGraph(data, do_parsing, with_answers, do_debugging):
    if do_parsing == True:
        print('Start transforming JSON to Query Graph')
        number_None = 0
        number_unparsable = 0
        number_no_topic_entity_and_no_core_inferential_chain = 0
        number_no_constraints = 0
        number_all_pass = 0
        new_data = []
        #answertype_set = set()
        progress_bar = progressbar.ProgressBar(max_value=len(data))
        for i, data_temp in enumerate(data): 
            ### data_temp是引用
            object_JSON_to_QueryGraph = class_JSON_to_QueryGraph()
            graph_temp = None
            topic_entity_temp = None
            core_inferential_chain_temp = None
            constraints_temp = None
            if data_temp['json'] == None: ### 原SPARQL无法解析成JSON
                count_add('原SPARQL无法解析成JSON')
                print("\"id\": %d, 原SPARQL无法解析成JSON\n" % (data_temp['id']))
                number_None += 1
            elif object_JSON_to_QueryGraph.generate_graph(data_temp['json']) == False: ### JSON无法解析成Query Graph
                print("\"id\": %d, JSON无法解析成Query Graph\n" % (data_temp['id']))
                number_unparsable += 1
                ###  无法解析可能的原因：不等式的操作数还是表达式；functionCall无法处理
            elif object_JSON_to_QueryGraph.get_topic_entity() == None: ### Query Graph无法提取topic entity和core inference chain
                print("\"id\": %d, Query Graph无法提取topic entity和core inference chain\n" % (data_temp['id']))
                graph_temp = copy.deepcopy(object_JSON_to_QueryGraph.graph)
                number_no_topic_entity_and_no_core_inferential_chain += 1
            elif object_JSON_to_QueryGraph.get_constraints() == None: ### Query Graph无法提取constraints
                print("\"id\": %d, Query Graph无法提取constraints\n" % (data_temp['id']))
                graph_temp = copy.deepcopy(object_JSON_to_QueryGraph.graph)
                topic_entity_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_topic_entity())
                core_inferential_chain_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_core_inferential_chain())
                count_add('core inference chain hop count = %d' % (len(core_inferential_chain_temp)))
                number_no_constraints += 1
            else:
                #print("\"id\": %d, 解析成功\n" % (data_temp['id']))
                graph_temp = copy.deepcopy(object_JSON_to_QueryGraph.graph)
                topic_entity_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_topic_entity())
                core_inferential_chain_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_core_inferential_chain())
                count_add('core inference chain hop count = %d' % (len(core_inferential_chain_temp)))
                constraints_temp = copy.deepcopy(object_JSON_to_QueryGraph.get_constraints())
                number_all_pass += 1

            #answertype_set.add(copy.deepcopy(data_temp['answertype']))
            new_data_temp = {}
            new_data_temp.update({'id': data_temp['id']})
            for question_temp in data_temp['question']:
                if question_temp['language'] == 'en':
                    question = copy.deepcopy(question_temp['string'])
                    keywords = copy.deepcopy(question_temp['keywords'])
                    break
            else:
                print('没有language为en的question')
                count_add('没有language为en的question')
                continue

            new_data_temp.update({'question': question})
            new_data_temp.update({'keywords': keywords})
            new_data_temp.update({'sparql': copy.deepcopy(data_temp['query']['sparql'])})

            if do_debugging == True:
                ### 调试用
                new_data_temp.update({'json': copy.deepcopy(data_temp['json'])})
                new_data_temp.update({'graph': copy.deepcopy(graph_temp)})
                new_data_temp.update({'graph_for_searching_constraints': copy.deepcopy(object_JSON_to_QueryGraph.graph_for_searching_constraints)})
            
            if with_answers == True:
                new_data_temp.update({'answers': copy.deepcopy(data_temp['answers'])})

            ### 从SPARQL得到的正确的Query Graph
            new_data_temp.update({'query_graph_golden': []})
            new_data_temp['query_graph_golden'].append({
                'topic_entity': copy.deepcopy(topic_entity_temp),
                'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp),
                'constraints': copy.deepcopy(constraints_temp)
            })
            if constraints_temp == []:
                count_add('constraints == []')
            if (constraints_temp != None) and (constraints_temp != []):
                for constraints_temp_temp in constraints_temp:
                    if constraints_temp_temp['type'] != 'triple':
                        break
                else:
                    count_add('constraints只有triple')
                    count_add('constraints只有triple，constraints数量 %d' % (len(constraints_temp)))

            new_data.append(copy.deepcopy(new_data_temp))

            progress_bar.update(i + 1)

        print('\n')
        print('The number of None JSON: ', number_None)
        print('The number of unparsable JSON: ', number_unparsable)
        print('The number of no_topic_entity_and_no_core_inferential_chain: ', number_no_topic_entity_and_no_core_inferential_chain)
        print('The number of no_constraints: ', number_no_constraints)
        print('The number of all_pass: ', number_all_pass)
        #print('answertype_set: ', answertype_set)
        print('count: \n', count)

        ### 保存数据
        f = open('data_in_QueryGraph.json', 'w')
        json.dump(obj=new_data, fp=f, indent=4)
        f.close()

    else:
        ### 读取数据
        f = open('data_in_QueryGraph.json', 'r')
        data = json.load(f) 
        f.close()

    return data

### 充分利用python语言在 函数传参 , = , for temp in data 中是传引用而不是传值，
### 因此修改被字典或列表赋值的变量，就修改了原始的字典或列表
class class_JSON_to_QueryGraph:
    def __init__(self):
        self.graph = None
        self.queue = []
        self.direction = ['out', 'in']
        self.and_not = ['and', 'not']
        self.topic_entity = None
        self.core_inferential_chain = None
        self.graph_for_searching_constraints = None ### 用于调试
        self.constraints = None

    ### generate_graph()返回True，则建立成功；generate_graph()返回False，则建立失败
    def generate_graph(self, data):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        ### 队列用于处理或逻辑运算符，算法基于BFS
        ### 队列元素为字典型，
        ### queue_temp['data']是JSON字典数据，用于当前分支节点的生成，
        ### queue_temp['graph']是空字典，用于存放当前分支节点的'and': {}, 'not': {}, 'or': []
        self.graph = {} ### 初始化 self.graph 为空字典
        self.queue.clear()
        self.queue.append({'data': data, 'graph': self.graph, 'and_not_index': 0})
        while len(self.queue) > 0:
            ### 出队
            queue_temp = self.queue[0]
            del self.queue[0]

            queue_temp['graph'].update({'queryType': None})
            queue_temp['graph'].update({'variables': None})          
            queue_temp['graph'].update({'and': {}})
            queue_temp['graph'].update({'not': {}})
            queue_temp['graph'].update({'or': []})
            queue_temp['graph'].update({'order_limit_offset': None})
            queue_temp['graph'].update({'group_having': None})  

            if self.all_type(queue_temp['data'], queue_temp['graph'], queue_temp['and_not_index']) == False:
                self.graph = None
                return False

        return True

    def all_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        if data['type'] == 'query':
            return self.query_type(data, graph, and_not_index)
        elif data['type'] == 'bgp':
            return self.bgp_type(data, graph, and_not_index)
        elif data['type'] == 'filter':
            return self.filter_type(data, graph, and_not_index)
        elif data['type'] == 'operation':
            return self.operation_type(data, graph, and_not_index)
        elif data['type'] == 'union':
            return self.union_type(data, graph, and_not_index)
        elif data['type'] == 'optional':
            return self.optional_type(data, graph, and_not_index)
        elif data['type'] == 'empty':
            return True
        elif data['type'] == 'group':
            return self.group_type(data, graph, and_not_index)
        else: ### 其他未知情况
            count_add('未知data[\'type\'] self.graph')
            print('未知data[\'type\']')
            return False

    def query_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        ### QALD 中 data['queryType'] 为 'SELECT' 或 'ASK'
        graph['queryType'] = copy.deepcopy(data['queryType'])
        if data['queryType'] == 'SELECT':
            graph['variables'] = []
            for variables_temp in data['variables']:
                ### 不处理SELECT查询变量形如 SELECT (( year(xsd:date(?end)) - year(xsd:date(?start)) ) AS ?years) 的SPARQL
                if 'expression' in variables_temp:
                    count_add('SELECT查询变量中包含expression self.graph')
                    print('SELECT查询变量中包含expression')
                    return False
                graph['variables'].append(copy.deepcopy(variables_temp))
        ### 默认'where'一定存在
        for where_temp in data['where']:
            if self.all_type(where_temp, graph, and_not_index) == False:
                return False
        if 'order' in data:
            if self.order_limit_offset(data, graph, and_not_index) == False:
                return False
        if ('group' in data) and ('having' in data):
            ### 聚合语句 GROUP BY ... HAVING ... 中，'group' 和 'having' 一定成对出现
            if self.group_having(data, graph, and_not_index) == False:
                return False

        return True

    def bgp_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        for triples_temp in data['triples']:
            ### 'Literal'类型不能作顶点
            if triples_temp['subject']['termType'] != 'Literal':
                ### 把三元组的主语、谓语添加到graph[self.and_not[and_not_index]]键中
                if triples_temp['subject']['value'] not in graph[self.and_not[and_not_index]]:
                    graph[self.and_not[and_not_index]].update({copy.deepcopy(triples_temp['subject']['value']): {
                        'termType': copy.deepcopy(triples_temp['subject']['termType']),
                        'edge': []
                    }})
                ### 把三元组添加到graph[self.and_not[and_not_index]]值中
                graph[self.and_not[and_not_index]][triples_temp['subject']['value']]['edge'].append({
                    'type': 'triple', ### 边的类型
                    'direction': 'out', ### 当前顶点的类型
                    'predicate': copy.deepcopy(triples_temp['predicate']), ### 谓语
                    'operator': None,
                    'endpoint': copy.deepcopy(triples_temp['object']) ### 边的另一端顶点
                })

            if triples_temp['object']['termType'] != 'Literal':
                if triples_temp['object']['value'] not in graph[self.and_not[and_not_index]]:
                    graph[self.and_not[and_not_index]].update({copy.deepcopy(triples_temp['object']['value']): {
                        'termType': copy.deepcopy(triples_temp['object']['termType']),
                        'edge': []
                    }})
                graph[self.and_not[and_not_index]][triples_temp['object']['value']]['edge'].append({
                    'type': 'triple',
                    'direction': 'in',
                    'predicate': copy.deepcopy(triples_temp['predicate']),
                    'operator': None,
                    'endpoint': copy.deepcopy(triples_temp['subject'])
                })

        return True

    def filter_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        if data['expression']['type'] == 'operation':
            return self.operation_type(data['expression'], graph, and_not_index)
        
        ### 未知错误
        count_add('未知data[\'expression\'][\'type\'] self.graph')
        print('未知data[\'expression\'][\'type\']')
        return False

    def operation_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        ### 运算符 '<', '>', '>=', '<=', '=', '!=' 都是2操作数，非逻辑运算符
        if (data['operator'] == '<') or (data['operator'] == '>') or (data['operator'] == '>=') or \
            (data['operator'] == '<=') or (data['operator'] == '=') or (data['operator'] == '!='):
            number_args = 2
            for i in range(number_args):
                ### 如果操作数有一方不是单一节点，该SPARQL不处理
                ### 形如 FILTER(xsd:dateTime(?pFrom) - xsd:dateTime(?from) > 0)
                ### 形如 FILTER (xsd:dateTime(?d) - xsd:dateTime(?from) >= 0)
                if 'termType' not in data['args'][i]:
                    count_add('不等式操作数有expression self.graph')
                    print('不等式操作数有expression')
                    return False
            for i in range(number_args):
                if data['args'][i]['termType'] != 'Literal':
                    ### 把二元不等式的2个操作数添加到graph[self.and_not[and_not_index]]键中
                    if data['args'][i]['value'] not in graph[self.and_not[and_not_index]]:
                        graph[self.and_not[and_not_index]].update({copy.deepcopy(data['args'][i]['value']): {
                            'termType': copy.deepcopy(data['args'][i]['termType']),
                            'edge': []
                        }})

                    graph[self.and_not[and_not_index]][data['args'][i]['value']]['edge'].append({
                        'type': 'operation',
                        'direction': self.direction[i], ### 边的方向，当前顶点是左操作数，'direction': 'out'；当前顶点是右操作数，'direction': 'in'
                        'predicate': None,
                        'operator': data['operator'], ### 运算符
                        'endpoint': copy.deepcopy(data['args'][self.reverse(i)])
                    })

            return True

        ### 算术运算符 '+', '-', '*', '/' 都是2操作数
        ### QALD的算术运算符只出现在'type': 'query'的'variables':[]

        ### 正则表达式 'regex' 都是2操作数，第1个操作数为 "termType": "Variable" ，第2个操作数为 "termType": "Literal"
        elif data['operator'] == 'regex':
            if data['args'][0]['termType'] != 'Literal':
                ### 把正则表达式的第1个操作数添加到graph[self.and_not[and_not_index]]键中
                if data['args'][0]['value'] not in graph[self.and_not[and_not_index]]:
                    graph[self.and_not[and_not_index]].update({copy.deepcopy(data['args'][0]['value']): {
                        'termType': copy.deepcopy(data['args'][0]['termType']),
                        'edge': []
                    }})

                graph[self.and_not[and_not_index]][data['args'][0]['value']]['edge'].append({
                    'type': 'operation',
                    'direction': self.direction[0], ### 增加方向性，当前顶点是左操作数，'direction': 'out'；当前顶点是右操作数，'direction': 'in'
                    'predicate': None,
                    'operator': data['operator'],
                    'endpoint': copy.deepcopy(data['args'][1])
                })

            return True

        ### 且运算符，依次处理每一个操作数（表达式）即可
        elif data['operator'] == '&&':
            if self.and_not[and_not_index] == 'and':
                for args_temp in data['args']:
                    if self.all_type(args_temp, graph, and_not_index) == False:
                        return False
            else: ### self.and_not[and_not_index] == 'not'，非且 等于 或非，必须把非放到子式上！
                graph['or'].append([])
                for args_temp in data['args']:
                    graph['or'][-1].append({})
                    self.queue.append({'data': args_temp, 'graph': graph['or'][-1][-1], 'and_not_index': self.reverse(and_not_index)})
            
            return True

        ### 或运算符，
        ### 先在graph['or']中为这个或运算符添加一个空列表
        ### 为每一个操作数在graph['or'][-1]添加一个空字典，并把每一个操作数和空字典加入队列
        elif data['operator'] == '||':
            if self.and_not[and_not_index] == 'and':
                graph['or'].append([])
                for args_temp in data['args']:
                    graph['or'][-1].append({})
                    self.queue.append({'data': args_temp, 'graph': graph['or'][-1][-1], 'and_not_index': and_not_index})
            else: ### self.and_not[and_not_index] == 'not'，非或 等于 且非，必须把非放到子式上！
                for args_temp in data['args']:
                    if self.all_type(args_temp, graph, self.reverse(and_not_index)) == False:
                        return False
            return True

        ### 非运算符，都是1操作数
        elif data['operator'] == '!':
            return self.all_type(data['args'][0], graph, self.reverse(and_not_index))

        ### 非运算符，都是1操作数
        elif data['operator'] == 'exists':
            return self.all_type(data['args'][0], graph, and_not_index)

        ### 非运算符，都是1操作数
        elif data['operator'] == 'notexists':
            return self.all_type(data['args'][0], graph, self.reverse(and_not_index))
        ### 'notin', 'in'运算符先不考虑

        ### 未知错误
        else:
            count_add('未知data[\'operator\'] operator: %s self.graph' % (data['operator']))
            print('未知data[\'operator\']')
            return False

    def reverse(self, x):
        return (x + 1) % 2

    ### 等价于 或逻辑
    def union_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        if self.and_not[and_not_index] == 'and':
            graph['or'].append([])
            for patterns_temp in data['patterns']:
                graph['or'][-1].append({})
                self.queue.append({'data': patterns_temp, 'graph': graph['or'][-1][-1], 'and_not_index': and_not_index})
        else: ### self.and_not[and_not_index] == 'not'，非或 等于 且非，必须把非放到子式上！
            for patterns_temp in data['patterns']:
                self.all_type(patterns_temp, graph, self.reverse(and_not_index))

        return True

    def optional_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        if self.and_not[and_not_index] == 'and':
            graph['or'].append([])
            ### 经验证，'optional' 的 'patterns' 一定只有1个元素
            graph['or'][-1].append({})
            self.queue.append({'data': data['patterns'][0], 'graph': graph['or'][-1][-1], 'and_not_index': and_not_index})
            ### 'optional'相当于， {'patterns'} 'union' {空'graph'}
            graph['or'][-1].append({})
            self.queue.append({'data': self.get_empty_data(), 'graph': graph['or'][-1][-1],  'and_not_index': and_not_index})
        else: ### self.and_not[and_not_index] == 'not'，非或 等于 且非，必须把非放到子式上！
            ### 经验证，'optional' 的 'patterns' 一定只有1个元素
            return self.all_type(data['patterns'][0], graph, self.reverse(and_not_index))
        
        return True

    def get_empty_data(self):
        return {'type': 'empty'}

    ### 等价于 且逻辑
    def group_type(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        if self.and_not[and_not_index] == 'and':
            for patterns_temp in data['patterns']:
                if self.all_type(patterns_temp, graph, and_not_index) == False:
                    return False
        else: ### self.and_not[and_not_index] == 'not'，非且 等于 或非，必须把非放到子式上！
            graph['or'].append([])
            for patterns_temp in data['patterns']:
                graph['or'][-1].append({})
                self.queue.append({'data': patterns_temp, 'graph': graph['or'][-1][-1], 'and_not_index': self.reverse(and_not_index)})

        return True

    def order_limit_offset(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        graph['order_limit_offset'] = {}
        ### 经验证，所有data['order']列表的长度均为1
        ### 如果data有键'order'，那么一定有键'limit'。可能有键'offset'，也可能没有
        graph['order_limit_offset'].update({'order': copy.deepcopy(data['order'])})
        if 'limit' in data:
            graph['order_limit_offset'].update({'limit': copy.deepcopy(data['limit'])})
        else:
            ### 如果没有指定 'limit'，默认结果的数量为1
            graph['order_limit_offset'].update({'limit': 1})
        if 'offset' in data:
            graph['order_limit_offset'].update({'offset': copy.deepcopy(data['offset'])})
        else:
            ### 如果没有指定 'offset'，默认从第0个数据开始
            graph['order_limit_offset'].update({'offset': 0})
        
        return True

    def group_having(self, data, graph, and_not_index):
        ### 如果不打算改变原始数据，需要做深复制，保证数据安全
        data = copy.deepcopy(data)
        graph['group_having'] = {}
        graph['group_having'].update({'group': copy.deepcopy(data['group'])})
        graph['group_having'].update({'having': copy.deepcopy(data['having'])})

        return True

    ### self.topic_entity 的类型是 str ， self.core_inferential_chain 的类型是 list -> str
    def set_topic_entity_and_core_inferential_chain(self, topic_entity, core_inferential_chain):
        self.topic_entity = topic_entity
        self.core_inferential_chain = core_inferential_chain

    def get_topic_entity(self):
        if self.topic_entity == None:
            if self.graph == None:
                count_add('QueryGraph is None')
                print('QueryGraph is None')
                return None

            ### 为了数据安全，做深复制
            graph_temp = copy.deepcopy(self.graph)

            ### 目前只考虑SELECT查询，
            if graph_temp['queryType'] != 'SELECT':
                count_add('graph_temp[\'queryType\'] == %s graph_temp' % (graph_temp['queryType']))
                print('graph_temp[\'queryType\'] == %s' % (graph_temp['queryType']))
                return None
            
            ### 目前只考虑有1个查询变量
            query_variables = graph_temp['variables'][0]

            ### 目前只考虑在'and'逻辑内
            if query_variables['value'] not in graph_temp['and']:
                count_add('查询变量不在graph_temp[\'and\'] self.topic_entity')
                print('查询变量不在graph_temp[\'and\']')
                return None

            ### 基于BFS，core inference chain越短，优先级越高
            ### visited_vertex列表保存曾经已经入过队列的顶点，
            ### 保证曾经已经入过队列的顶点不再访问，防止在环子图中陷入死循环
            visited_vertex = []
            self.queue.clear()
            self.queue.append({'vertex': copy.deepcopy(query_variables), 'path': []})
            visited_vertex.append(copy.deepcopy(self.queue[-1]['vertex']))
            while len(self.queue) > 0:
                ### copy.deepcopy() 避免修改一方的同时，无意间修改另一方
                queue_temp = copy.deepcopy(self.queue[0])
                del self.queue[0]

                for edge_temp in graph_temp['and'][queue_temp['vertex']['value']]['edge']:
                    ### core_inferential_chain只考虑三元组
                    if edge_temp['type'] != 'triple':
                        continue
                    
                    ### 曾经已经入过队列的顶点不再访问，防止在环子图中陷入死循环
                    elif edge_temp['endpoint']['value'] in visited_vertex:
                        continue

                    ### topic entity必须是NamedNode，且不能是type
                    elif edge_temp['endpoint']['termType'] == 'NamedNode':
                        ### topic entity不能是 type
                        for edge_temp_temp in graph_temp['and'][edge_temp['endpoint']['value']]['edge']:
                            if ((edge_temp_temp['type'] == 'triple') and \
                                (len(edge_temp_temp['predicate']['value']) >= 5) and \
                                (edge_temp_temp['predicate']['value'][-5:] == '#type')):
                                break
                        else:
                            ### self.topic_entity 的类型是 str
                            self.topic_entity = self.append_mark(copy.deepcopy(edge_temp['endpoint']))
                            ### self.core_inferential_chain 的类型是 list -> dict，字典数据是三元组 {'subject': str, 'predicate': str, 'object': str}
                            if edge_temp['direction'] == 'out':
                                '''
                                copy.deepcopy(queue_temp['path']).append({
                                    'subject': copy.deepcopy(queue_temp['vertex']),
                                    'predicate': copy.deepcopy(edge_temp['predicate']),
                                    'object': copy.deepcopy(edge_temp['endpoint'])
                                }
                                最终结果为None，因为这里赋予的是append()的返回值，即为None，而不是列表数据本身
                                '''
                                path_temp = copy.deepcopy(queue_temp['path'])
                                path_temp.append({
                                    'subject': self.append_mark(copy.deepcopy(queue_temp['vertex'])),
                                    'predicate': self.append_mark(copy.deepcopy(edge_temp['predicate'])),
                                    'object': self.append_mark(copy.deepcopy(edge_temp['endpoint']))
                                })
                                self.core_inferential_chain = path_temp
                            elif edge_temp['direction'] == 'in':
                                path_temp = copy.deepcopy(queue_temp['path'])
                                path_temp.append({
                                    'subject': self.append_mark(copy.deepcopy(edge_temp['endpoint'])),
                                    'predicate': self.append_mark(copy.deepcopy(edge_temp['predicate'])),
                                    'object': self.append_mark(copy.deepcopy(queue_temp['vertex']))
                                })
                                self.core_inferential_chain = path_temp
                            else: ### 其他未知情况
                                count_add('三元组的顶点的角色未知（非subject、非object）')
                                print('三元组的顶点的角色未知（非subject、非object）')
                                self.topic_entity = None
                                self.core_inferential_chain = None
                                return None

                            ### 原本path的顺序是 查询变量 -> topic entity。因此需要对列表进行翻转，变成 topic entity -> 查询变量
                            self.core_inferential_chain.reverse()
                            return self.topic_entity
                    
                    ### 'Literal'是不能作为topic entity
                    elif edge_temp['endpoint']['termType'] == 'Literal':
                        continue

                    ### 只剩下edge_temp['endpoint']['termType'] == 'Variable' 或 
                    ### ((edge_temp_temp['type'] == 'triple') and (len(edge_temp_temp['predicate']['value']) >= 5) and (edge_temp_temp['predicate']['value'][-5:] == '#type'))
                    else:    
                        if edge_temp['direction'] == 'out':
                            path_temp = copy.deepcopy(queue_temp['path'])
                            path_temp.append({
                                'subject': self.append_mark(copy.deepcopy(queue_temp['vertex'])),
                                'predicate': self.append_mark(copy.deepcopy(edge_temp['predicate'])),
                                'object': self.append_mark(copy.deepcopy(edge_temp['endpoint']))
                            })
                        elif edge_temp['direction'] == 'in':
                            path_temp = copy.deepcopy(queue_temp['path'])
                            path_temp.append({
                                'subject': self.append_mark(copy.deepcopy(edge_temp['endpoint'])),
                                'predicate': self.append_mark(copy.deepcopy(edge_temp['predicate'])),
                                'object': self.append_mark(copy.deepcopy(queue_temp['vertex']))
                            })
                        else:  ### 其他未知情况
                            count_add('三元组的顶点的角色未知（非subject、非object）')
                            print('三元组的顶点的角色未知（非subject、非object）')
                            self.topic_entity = None
                            self.core_inferential_chain = None
                            return None

                        self.queue.append({
                            'vertex': copy.deepcopy(edge_temp['endpoint']), 
                            'path': path_temp
                        })
                        visited_vertex.append(copy.deepcopy(self.queue[-1]['vertex']['value']))
        
            else:  ### 其他未知情况
                count_add('从查询变量出发，无法搜索到合适的topic entity self.topic_entity')
                print('从查询变量出发，无法搜索到合适的topic entity')
                self.topic_entity = None
                self.core_inferential_chain = None
                return None
        
        else:
            return self.topic_entity

    ### BFS无法处理叶子节点相同，路径不同的情况
    ### DFS搜索topic entity和core inferential chain可以避免
    ### 之后要搜索多个core inferential chain时再改进代码！！！！！
    def search_core_inferential_chain(self, stack, vertex):
        stack.append(copy.deepcopy(vertex))
        for edge_temp in graph['and'][vertex['value']]['edge']:
            pass
        stack.pop()

    def get_core_inferential_chain(self):
        if self.core_inferential_chain == None:
            if self.graph == None:
                count_add('QueryGraph is None')
                print('QueryGraph is None')
                return None

            ### 如果没有自行设置self.topic_entity 和 self.core_inferential_chain，
            ### 那么就随机生成
            if self.get_topic_entity() == None:
                count_add('因为无法生成topic entity，所以无法生成core inference chain self.core_inferential_chain')
                print('因为无法生成topic entity，所以无法生成core inference chain')
                return None

            return self.core_inferential_chain
        else:
            return self.core_inferential_chain
    
    def get_constraints(self):
        if self.constraints == None:
            if self.graph == None:
                count_add('QueryGraph is None')
                print('QueryGraph is None')
                return None

            if self.get_topic_entity() == None:
                count_add('因为无法生成topic entity，所以无法生成constraints self.constraints')
                print('因为无法生成topic entity，所以无法生成constraints')
                return None
            
            if (len(self.graph['not']) != 0) or (len(self.graph['or']) != 0):
                count_add('因为包含非逻辑或或逻辑，所以无法生成constraints self.constraints')
                print('因为包含非逻辑或或逻辑，所以无法生成constraints')
                return None

            ### 为了数据安全，做深复制
            graph_temp = copy.deepcopy(self.graph)
            self.graph_for_searching_constraints = graph_temp ### 用于调试
            self.constraints = []

            ### 生成 core_inferential_chain 上的顶点依次排列的列表 core_inferential_chain_vertex_list
            ### 顺序是 topic entity -> 查询变量
            core_inferential_chain_vertex_list = []
            ### 利用self.core_inferential_chain的顺序是 topic entity -> 查询变量
            ### 为了在graph中查询，core_inferential_chain_vertex_list中的顶点不带mark
            core_inferential_chain_vertex_list.append(self.remove_mark(self.topic_entity))
            for i in range(len(self.core_inferential_chain)):
                if core_inferential_chain_vertex_list[-1] == self.remove_mark(self.core_inferential_chain[i]['subject']):
                    core_inferential_chain_vertex_list.append(self.remove_mark(copy.deepcopy(self.core_inferential_chain[i]['object'])))
                elif core_inferential_chain_vertex_list[-1] == self.remove_mark(self.core_inferential_chain[i]['object']): 
                    core_inferential_chain_vertex_list.append(self.remove_mark(copy.deepcopy(self.core_inferential_chain[i]['subject'])))
                else: ### 其他未知情况
                    #print('core_inferential_chain_vertex_list[-1]: ', core_inferential_chain_vertex_list[-1])
                    #print('self.remove_mark(self.core_inferential_chain[i][\'subject\']): ', self.remove_mark(self.core_inferential_chain[i]['subject']))
                    #print('self.remove_mark(self.core_inferential_chain[i][\'subject\']): ', self.remove_mark(self.core_inferential_chain[i]['object']))
                    count_add('三元组的顶点的角色未知（非subject、非object） self.constraints')
                    print('三元组的顶点的角色未知（非subject、非object）')
                    self.constraints = None
                    return None
            
            '''
            收缩变量端点，把core inferential chain连接的变量端点去掉
            切记删除core inferential chain连接变量端点的边，变量端点可能在core inferential chain上，也可能不在
            主要考虑
            ?uri dbo:populationTotal ?inhabitants .
            FILTER (?inhabitants > 100000) .
            '''
            ### 不考虑topic entity
            ### 记录已访问的变量顶点，防止在变量回路中无限循环
            stack = copy.deepcopy(core_inferential_chain_vertex_list[1:])
            for core_inferential_chain_vertex_list_temp in core_inferential_chain_vertex_list[1:]:
                remove_list = []
                for i, edge_temp in enumerate(graph_temp['and'][core_inferential_chain_vertex_list_temp]['edge']):
                    ### 端点为变量，条件与 core inference chain 的距离大于1跳
                    if edge_temp['endpoint']['termType'] == 'Variable':
                        ### 如果变量不在 core inferential chain 中，就进行端点收缩
                        if edge_temp['endpoint']['value'] not in stack:
                            ### graph_temp 传引用
                            self.contract_Variable_vertex(graph_temp, core_inferential_chain_vertex_list_temp, edge_temp, stack, edge_temp['endpoint']['value'])
                    
                        ### 不能在此处直接用 graph_temp['and'][core_inferential_chain_vertex_list_temp]['edge'].remove(edge_temp)
                        ### 会改变 graph_temp['and'][core_inferential_chain_vertex_list_temp]['edge'] 的内容，从而影响后续遍历
                        remove_list.append(i)
                ### 必须从后往前删除
                remove_list.reverse()
                for remove_list_temp in remove_list:
                    graph_temp['and'][core_inferential_chain_vertex_list_temp]['edge'].pop(remove_list_temp)
                ### 清除无用变量，防止内存泄漏
                remove_list.clear()
                            
            ### 清除无用变量，防止内存泄漏
            stack.clear()

            ### 按照WebQSP的 "Constraints" 表示方法
            ### 搜索'and'逻辑中，core inference chain上的顶点连接的所有条件（1跳），这些条件排除了core inference chain包含的三元组
            ### 不等式运算符和'operator'的映射关系
            ### QALD中没有出现EXISTS和NOTEXISTS，因此这里不考虑。（在上面设计的Query Graph中，EXISTS和NOTEXISTS作为逻辑运算处理）
            operator_to_operator = {'=': 'Equal', '!=': 'NotEqual', '<': 'LessThan', '>': 'GreaterThan', '<=': 'LessOrEqual', '>=': 'GreaterOrEqual'}
            ### 不给topic entity设置constraints
            for i in range(1, len(core_inferential_chain_vertex_list)):
                for edge_temp in graph_temp['and'][core_inferential_chain_vertex_list[i]]['edge']:
                    constraints_temp = {
                        'type': None,
                        'Operator': None,
                        'Direction': None,
                        'ArgumentType': None,
                        'Argument': None,
                        'EntityName': None,
                        'SourceNodeIndex': None,
                        'NodePredicate': None,
                        'ValueType': None
                    }

                    ### 边指向的顶点在core inference chain中，且在core inference chain中，当前顶点与边指向的顶点相邻。这种情况不添加在constraints中
                    if (edge_temp['type'] == 'triple') and \
                        (((i > 0) and (edge_temp['endpoint']['value'] == core_inferential_chain_vertex_list[i - 1])) or
                        ((i < (len(core_inferential_chain_vertex_list) - 1)) and (edge_temp['endpoint']['value'] == core_inferential_chain_vertex_list[i + 1]))):
                        continue

                    if edge_temp['type'] == 'triple':
                        constraints_temp['type'] = copy.deepcopy(edge_temp['type'])
                        constraints_temp['Operator'] = 'Equal'
                        ### 增加方向性，当前顶点做主语，'direction': 'out'；当前顶点做宾语，'direction': 'in'
                        constraints_temp['Direction'] = copy.deepcopy(edge_temp['direction'])
                    elif edge_temp['type'] == 'operation':
                        constraints_temp['type'] = copy.deepcopy(edge_temp['type'])
                        constraints_temp['Operator'] = copy.deepcopy(operator_to_operator[edge_temp['operator']])
                        ### 增加方向性，当前顶点是左操作数，'direction': 'out'；当前顶点是右操作数，'direction': 'in'
                        constraints_temp['Direction'] = copy.deepcopy(edge_temp['direction'])
                    else: ### 其他未知情况
                        count_add('有除了triple、operation以外的其他边 self.constraints')
                        print('有除了triple、operation以外的其他边')
                        self.constraints = None
                        return None
                    '''
                    已通过预处理，去除掉这种情况
                    ### 边指向的顶点的类型是'termType': 'Variable'，这种情况不添加在constraints中
                    if edge_temp['endpoint']['termType'] == 'Variable':
                        ### 端点为变量，条件与 core inference chain 的距离大于1跳
                        count_add('端点为变量，条件与 core inference chain 的距离大于1跳 self.constraints')
                        print('端点为变量，条件与 core inference chain 的距离大于1跳')
                        self.constraints = None
                        return None
                    '''
                    if edge_temp['endpoint']['termType'] == 'NamedNode':
                        constraints_temp['ArgumentType'] = 'Entity'
                    elif edge_temp['endpoint']['termType'] == 'Literal':
                        constraints_temp['ArgumentType'] = 'Value'
                        ### WebQSP的 "ValueType"
                        ### 这是在DBpedia中，暂时观察到的规律
                        if 'string' in edge_temp['endpoint']['datatype']['value'].lower():
                            constraints_temp['ValueType'] = 'String'
                        elif 'date' in edge_temp['endpoint']['datatype']['value'].lower():
                            constraints_temp['ValueType'] = 'DateTime'
                        else:
                            constraints_temp['ValueType'] = 'Number'

                    else: ### 其他未知情况
                        count_add('endpoint类型为Variable或未知（非NamedNode、非Literal） self.constraints')
                        print('endpoint类型为Variable或未知（非NamedNode、非Literal）')
                        self.constraints = None
                        return None
                    
                    constraints_temp['Argument'] = self.append_mark(copy.deepcopy(edge_temp['endpoint']))
                    ### WebQSP的 "EntityName" ，未知，无法设置
                    constraints_temp['EntityName'] = None

                    ### core inference chain 中，从 topic entity 的下一个顶点开始标号（第1个标号为0）
                    constraints_temp['SourceNodeIndex'] = i - 1
                    ### WebQSP的 "NodePredicate" ，三元组有谓语、大于1跳的变量连接的条件也可能有谓语
                    ### if edge_temp['type'] == 'triple': 该条件不能涵盖全部
                    if edge_temp['predicate'] != None:
                        constraints_temp['NodePredicate'] = self.append_mark(copy.deepcopy(edge_temp['predicate']))
                    else:
                        constraints_temp['NodePredicate'] = edge_temp['predicate']

                    self.constraints.append(copy.deepcopy(constraints_temp))

            ### 处理条件 'order_limit_offset'
            ### 按照WebQSP的 "Order" 表示方法
            if graph_temp['order_limit_offset'] != None:
                order_limit_offset = {
                    'type': None,
                    'ValueType': None,
                    'SourceNodeIndex': None,
                    'NodePredicate': None,
                    'SortOrder': None,
                    'Start': None,
                    'Count': None
                }
                order_limit_offset.update({'type': 'order_limit_offset'})
                ### 只处理 graph_temp['order_limit_offset']['order'] 有1个表达式的情况
                ### 经统计 QALD 中所有'order'的列表的元素数量为1
                if len(graph_temp['order_limit_offset']['order']) > 1:
                    count_add('len(graph_temp[\'order_limit_offset\'][\'order\']) > 1 self.constraints')
                    print('len(graph_temp[\'order_limit_offset\'][\'order\']) > 1')
                    self.constraints = None
                    return None

                '''
                QALD只有2种"order"
                第1种是
                "order": [
                    {
                        "expression": {
                            "termType": "Variable",
                            "value": "elevation"
                        },
                        "descending": true
                    }
                ]
                标志是 'termType' in graph_temp['order_limit_offset']['order'][0]['expression']
                用于排序的顶点是 graph_temp['order_limit_offset']['order'][0]['expression']['value']
                order_limit_offset.update({'ValueType': 'String'})
                第2种是，其中取值固定的字段是 "type": "aggregate", "aggregation": "count
                "order": [
                    {
                        "expression": {
                            "expression": {
                                "termType": "Variable",
                                "value": "film"
                            },
                            "type": "aggregate",
                            "aggregation": "count",
                            "distinct": false
                        },
                        "descending": true
                    }
                ]
                标志是 'termType' not in graph_temp['order_limit_offset']['order'][0]['expression']
                用于排序的顶点是 graph_temp['order_limit_offset']['order'][0]['expression']['expression']['value']
                order_limit_offset.update({'ValueType': 'Number'})
                '''

                if 'termType' in graph_temp['order_limit_offset']['order'][0]['expression']:
                    ### 根据观察数据集QALD可得，变量名为'date'时，'ValueType': 'DateTime'
                    if graph_temp['order_limit_offset']['order'][0]['expression']['value'] == 'date':
                        order_limit_offset['ValueType'] = 'DateTime'
                    else:
                        order_limit_offset['ValueType'] = 'String'
                    vertex = graph_temp['order_limit_offset']['order'][0]['expression']['value']
                else:
                    ### 根据观察可得，双重'expression'中，第一层'expression'一般是聚合函数，例如count。因此'ValueType': 'Number'
                    vertex = graph_temp['order_limit_offset']['order'][0]['expression']['expression']['value']
                    order_limit_offset['ValueType'] = 'Number'

                ### 在core inference chain的顶点中，找到离topic entity最近的 且 与排序所用顶点vertex用三元组相连的顶点（1跳）
                for i, core_inferential_chain_vertex_list_temp in enumerate(core_inferential_chain_vertex_list[1:]):
                    ### 用core inferential chain上的变量排序
                    if vertex == core_inferential_chain_vertex_list_temp:
                        order_limit_offset['SourceNodeIndex'] = copy.deepcopy(i)
                        order_limit_offset['NodePredicate'] = None
                        break
                    else:
                        ### core inferential chain连接变量的边可能在变量端点收缩中被删除，因此需要从vertex的方向搜索
                        for edge_temp in graph_temp['and'][vertex]['edge']:
                            if edge_temp['endpoint']['value'] == core_inferential_chain_vertex_list_temp:
                                order_limit_offset['SourceNodeIndex'] = copy.deepcopy(i)
                                order_limit_offset['NodePredicate'] = self.append_mark(copy.deepcopy(edge_temp['predicate']))
                        ### 跳出双循环的方法！
                                break
                        else:
                            continue
                        break
                else: ### 在core inference chain的顶点中，找不到要求的顶点
                    order_limit_offset['SourceNodeIndex'] = None
                    order_limit_offset['NodePredicate'] = None

                if 'descending' not in graph_temp['order_limit_offset']['order'][0]:
                    ### 默认升序
                    order_limit_offset['SortOrder'] = 'Ascending'
                elif graph_temp['order_limit_offset']['order'][0]['descending'] == True:
                    order_limit_offset['SortOrder'] = 'Descending'
                else:
                    order_limit_offset['SortOrder'] = 'Ascending'
                order_limit_offset['Start'] = copy.deepcopy(graph_temp['order_limit_offset']['offset'])
                order_limit_offset['Count'] = copy.deepcopy(graph_temp['order_limit_offset']['limit'])

                self.constraints.append(copy.deepcopy(order_limit_offset))

            ### 在WebQSP中，放弃了 GROUP BY ... HAVING ... 
            ### 因此这里对 graph_temp['group_having'] != None 的QUeryGraph不作处理
            if graph_temp['group_having'] != None:
                count_add('有GROUP BY ... HAVING ...语句 self.constraints')
                print('有GROUP BY ... HAVING ...语句')
                self.constraints = None
                return None
            
            return self.constraints, graph_temp
        
        else:
            return self.constraints
    
    ### vertex的类型是dict
    def append_mark(self, vertex):
        ### 变量
        if vertex['termType'] == 'Variable':
            return '?' + vertex['value']
        ### 实体或类型URI
        elif vertex['termType'] == 'NamedNode':
            return '<' + vertex['value'] + '>'
        ### Literal
        elif vertex['termType'] == 'Literal':
            if 'string' in vertex['datatype']['value'].lower(): ### 'String'类型
                if vertex['language'] != '':
                    return '\"' + vertex['value'] + '\"' + '@' + vertex['language']
                else:
                    return '\"' + vertex['value'] + '\"'
            elif 'date' in vertex['datatype']['value'].lower(): ### 'DateTime'类型
                return '\"' + vertex['value'] + '\"' + '^^' + '<' + vertex['datatype']['value'] + '>'
            else: ### 'Number'类型
                return '\"' + vertex['value'] + '\"' + '^^' + '<' + vertex['datatype']['value'] + '>'
        else:
            print('未知vertex类型')
            return vertex['value']

    ### vertex的类型是str
    def remove_mark(self, vertex):
        ### 变量
        if vertex[0] == '?':
            return vertex[1:]
        ### 实体或类型URI
        elif (vertex[0] == '<') and (vertex[-1] == '>'):
            return vertex[1:-1]
        ### Literal
        elif vertex[0] == '\"':
            return vertex.split('\"')[0]
        else:
            print('未知vertex类型')
            return vertex

    def contract_Variable_vertex(self, graph, initial_vertex, initial_edge, stack, vertex):
        stack.append(copy.deepcopy(vertex))
        for edge_temp in graph['and'][vertex]['edge']:
            ### 端点仍为变量
            if (edge_temp['endpoint']['termType'] == 'Variable') and (edge_temp['endpoint']['value'] not in stack):
                self.contract_Variable_vertex(graph, initial_vertex, initial_edge, stack, edge_temp['endpoint']['value'])
            elif edge_temp['endpoint']['termType'] != 'Variable':
                dictionary = {
                    'type': copy.deepcopy(edge_temp['type']),
                    ### 使用初始边的'direction'，默认最终端点的'opreation'方向为'out'
                    'direction': copy.deepcopy(initial_edge['direction']),
                    'predicate': copy.deepcopy(initial_edge['predicate']),
                    'operator': copy.deepcopy(edge_temp['operator']),
                    'endpoint': copy.deepcopy(edge_temp['endpoint'])
                }
                graph['and'][initial_vertex]['edge'].append(copy.deepcopy(dictionary))
                dictionary.clear()
        stack.pop()

if __name__=="__main__":
    #data_path = 'data/QALD/train-multilingual-4-9.json'
    data_path = 'data/QALD/test-multilingual-merged.json'
    do_read_SPARQL = True
    do_SPARQL_to_JSON = True
    do_JSON_to_QueryGraph = True
    with_answers = True
    do_debugging = False

    data = read_SPARQL(data_path, do_read_SPARQL)
    data = SPARQL_to_JSON(data, do_SPARQL_to_JSON)
    data = JSON_to_QueryGraph(data, do_JSON_to_QueryGraph, with_answers, do_debugging)