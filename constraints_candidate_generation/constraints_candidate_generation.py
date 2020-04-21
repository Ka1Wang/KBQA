def read_QueryGraph():
    f = open('../SPARQL_to_QueryGraph/train_merged_QALD_in_QueryGraph.json', 'r')
    QueryGraph = json.load(f) ### data为list->dict类型
    f.close()

    return QueryGraph

'''
"3.05277E11"^^<http://dbpedia.org/datatype/usDollar>
符号^^后面的URI表示前面Literal的数据类型
"Czech writer and journalist"@en
符号@后面的en表示前面String的语言类型
'''
import os
import sys
import copy
import json
import progressbar
import psutil
import tarfile
### memory_limit的单位是GB
class class_DBpedia_graph:
    def __init__(self, make_graph, DBpedia_path='../DBpedia', memory_limit=40, \
        file_name_list = [
            'article_categories_en.ttl',
            'instance_types_en.ttl',
            'infobox_properties_en.ttl',
            'mappingbased_literals_en.ttl',
            'mappingbased_objects_en.ttl',
            'persondata_en.ttl',
            'infobox_properties_mapped_en.ttl',
            'instance_types_transitive_en.ttl',
            #'category_labels_en.ttl',
        ] \
    ):

        self.DBpedia_path = DBpedia_path
        self.memory_limit = memory_limit
        self.process = psutil.Process(os.getpid())
        if make_graph == True:
            self.generate_DBpedia_graph(file_name_list)
        
        ### 读取DBpedia_graph
        '''
        用set()建立key_set的原因是，在判断某个元素是否存在时，对顺序没有要求，list可以保持顺序确定，set不能保持顺序确定
        而使用in list或not in list的时间复杂度是O(N)，使用in set或not in set的时间复杂度是O(1)
        因此建立key_set用于判断某个元素是否存在，时间复杂度更低
        '''
        self.DBpedia_graph_file = tarfile.open(os.path.join(self.DBpedia_path, 'DBpedia_graph.tar'), 'r')
        self.entity_map = json.load(self.DBpedia_graph_file.extractfile('entity_map.json')) ### self.entity_map的类型是dict，键为entity，值为子文件名
        self.entity_map_key_set = set(self.entity_map.keys())
        self.edge_cache = {}
        self.edge_cache_key_set = set()
        self.endpoint_set_cache = {}
        self.endpoint_set_cache_key_set = set()
        self.endpoint_index_cache = {}
        self.endpoint_index_cache_key_set = set()
        

    '''
    为每一个entity都建立1个存储edge的文件（edge文件很小），最后建立1个entity到edge文件的HASH表
    在建立的过程中，需要频繁地读写文件，因此比较慢。由于三元组存储的局部性原理，不会慢太多！
    在使用的过程中，因为每一个question只涉及很少量的entity，所以只需要读取很少量的edge文件（edge文件很小）
    '''
    def generate_DBpedia_graph(self, file_name_list):
        print('Start making DBpedia graph')
        ### 创建临时目录
        if os.path.exists(os.path.join(self.DBpedia_path, 'DBpedia_graph')) == False:
            os.makedirs(os.path.join(self.DBpedia_path, 'DBpedia_graph'))
        else:
            '''
            print('Remove DBpedia_graph. You can also execute \"rm -r %s\" for higher efficiency.' % (os.path.join(self.DBpedia_path, 'DBpedia_graph')))
            ### os.removedirs() 是递归删除空目录，在目录中不能有其他类型的文件文件
            file_name = os.listdir(os.path.join(self.DBpedia_path, 'DBpedia_graph'))
            for file_name_temp in file_name:
                os.remove(os.path.join(self.DBpedia_path, 'DBpedia_graph', file_name_temp))
            os.removedirs(os.path.join(self.DBpedia_path, 'DBpedia_graph'))
            '''
            ### 必须删除旧临时目录。1.之后以'a'模式写入，旧数据还在 2.旧文件的数量可能多于新文件，之后打包到.tar文件时，会打包多余的文件
            os.system('rm -r %s' % (os.path.join(self.DBpedia_path, 'DBpedia_graph')))
            os.makedirs(os.path.join(self.DBpedia_path, 'DBpedia_graph'))
        '''
        ### 删除旧 DBpedia.tar，可以省略。因为之后以'w'模式写入，会覆盖旧数据
        if os.path.exists(os.path.join(self.DBpedia_path, 'DBpedia_graph.tar')) == True:
            os.system('rm -r %s' % (os.path.join(self.DBpedia_path, 'DBpedia_graph')))
        '''
        file_name = os.listdir(self.DBpedia_path)
        self.entity_map = {}
        DBpedia_graph_temp = {}
        #process_number = 1 ### 为了实验效率，只处理process_number个文件
        for file_name_temp in file_name:
            if file_name_temp not in file_name_list:
                continue
            '''
            ### 为了实验效率，只处理process_number个文件
            process_number -= 1
            if process_number < 0:
                break
            '''
            print('Process %s' % (file_name_temp))
            '''
            f = open(os.path.join(self.DBpedia_path, file_name_temp), 'r')
            f.readlines()会把文件内容一次性全部读入，占据大量内存
            f_readlines = f.readlines() ### list的每一个元素为str，对应1行内容。除了最后1行，其余所有行的str以'\n'结尾。
            f.close()
            '''
            ### 统计文件行数
            f = open(os.path.join(self.DBpedia_path, file_name_temp), 'r')
            f_length = 0
            while f.readline() != '':
                f_length += 1
            f.close()

            f = open(os.path.join(self.DBpedia_path, file_name_temp), 'r')
            progress_bar = progressbar.ProgressBar(max_value=f_length)
            for i in range(f_length):
                ### readline() 方法用于从文件读取整行，包括 '\n' 字符
                f_readline_temp = f.readline()
                ### 首字符是'#'，则为注释行。首行和尾行一般是注释行
                if f_readline_temp[0] == '#':
                    continue
                '''
                ### 测试用
                if i > 100:
                    break
                '''
                ### 分割字符串
                '''
                f_readline_temp.split()
                无法处理 <http://dbpedia.org/resource/Don_Shy> <http://purl.org/dc/terms/description> "American football player & World-Class hurdler. In 1966,Shy was the # 2 ranked 110 meter hurdler in the world."@en .
                也就是无法处理 Literal
                '''
                ### 以' ', '<', '>', '\"', '.'为分割标志字符
                ### 这个版本较复杂，但是没有Literal位置的前提假设
                ### 结果把'.'丢弃
                f_readline_temp_split = []
                left = 0
                right = 0
                while len(f_readline_temp_split) < 3:
                    if (f_readline_temp[right] == ' ') and \
                        (((right > 0) and ((f_readline_temp[right - 1] == '>') or (f_readline_temp[right - 1] == '\"'))) or \
                        ((right < len(f_readline_temp) - 1) and ((f_readline_temp[right + 1] == '<') or (f_readline_temp[right + 1] == '\"') or (f_readline_temp[right + 1] == '.')))):
                        f_readline_temp_split.append(copy.deepcopy(f_readline_temp[left:right]))
                        left = right + 1
                    right += 1
                #print(f_readline_temp_split)
                #exit(0)

                '''
                ### 这个版本假设Literal（格式为"XX"@XX或"XX"^^<XX>）只可能出现在object上，其他情况均为URI（格式为<XX>）
                f_readline_temp_split_temp = f_readline_temp.split()
                ### subject 和 predicate不可能是Literal。Literal只可能出现在object
                ### 结果把'.'丢弃
                f_readline_temp_split = [
                    copy.deepcopy(f_readline_temp_split_temp[0]), 
                    copy.deepcopy(f_readline_temp_split_temp[1])
                ]
                if f_readline_temp_split_temp[2][0] == '<':
                    f_readline_temp_split.append(copy.deepcopy(f_readline_temp_split_temp[2]))
                else: ### object是Literal
                    object_temp = ''
                    for j in range(2, (len(f_readline_temp_split_temp) - 1)): ### 忽略掉最后1个'.'
                        object_temp += copy.deepcopy(f_readline_temp_split_temp[j]) + ' '
                    else:
                        object_temp.rstrip() ### 删除字符串末尾的空格
                    f_readline_temp_split.append(object)
                '''
                ### 首字符是'<'，则为entity或type
                ### 首字符是'"'或'\"'，则为literal
                if f_readline_temp_split[0][0] == '<':
                    if f_readline_temp_split[0] not in self.entity_map:
                        self.entity_map.update({copy.deepcopy(f_readline_temp_split[0]): '%d.json' % (len(self.entity_map) + 1)})

                    if f_readline_temp_split[0] not in DBpedia_graph_temp:
                        DBpedia_graph_temp.update({copy.deepcopy(f_readline_temp_split[0]): []})

                    dictionary_temp = {}
                    ### 当前顶点是主语，'direction': 'out'
                    dictionary_temp.update({'direction': 'out'})
                    dictionary_temp.update({'predicate': copy.deepcopy(f_readline_temp_split[1])})
                    dictionary_temp.update({'endpoint': copy.deepcopy(f_readline_temp_split[2])})
                    DBpedia_graph_temp[f_readline_temp_split[0]].append(copy.deepcopy(dictionary_temp))

                if f_readline_temp_split[2][0] == '<':
                    if f_readline_temp_split[2] not in self.entity_map:
                        self.entity_map.update({copy.deepcopy(f_readline_temp_split[2]): '%d.json' % (len(self.entity_map) + 1)})

                    if f_readline_temp_split[2] not in DBpedia_graph_temp:
                        DBpedia_graph_temp.update({copy.deepcopy(f_readline_temp_split[2]): []})

                    dictionary_temp = {}
                    ### 当前顶点是宾语，'direction': 'in'
                    dictionary_temp.update({'direction': 'in'})
                    dictionary_temp.update({'predicate': copy.deepcopy(f_readline_temp_split[1])})
                    dictionary_temp.update({'endpoint': copy.deepcopy(f_readline_temp_split[0])})
                    DBpedia_graph_temp[f_readline_temp_split[2]].append(copy.deepcopy(dictionary_temp))

                ### 当该进程占用内存超过一定限额时，把DBpedia_graph_temp保存，防止内存超限、程序终止
                ### 当前程序对应的进程不会改变，为了减少时间花费，只在类初始化时创建一次
                #process = psutil.Process(os.getpid())
                ### process.memory_info().rss的单位是字节
                if self.process.memory_info().rss >= (self.memory_limit * (2**30)): 
                    self.write_DBpedia_graph(DBpedia_graph_temp)
                    DBpedia_graph_temp.clear()

                progress_bar.update(i + 1)
            print('\n')

            ### 清除内存中多余的变量，防止内存泄漏，但是该进程并不会把这部分内存资源返还给系统
            f.close()
            ### f_readline_temp未处理
            f_readline_temp_split.clear()
            dictionary_temp.clear()

        ### 把剩余的DBpedia_graph_temp保存
        self.write_DBpedia_graph(DBpedia_graph_temp)
        DBpedia_graph_temp.clear()

        ### 把临时的TXT文件转化为最终的JSON文件
        print('Transform .txt to .json')
        progress_bar = progressbar.ProgressBar(max_value=len(self.entity_map))
        for i, entity_map_values_temp in enumerate(self.entity_map.values()):
            f = open(os.path.join(self.DBpedia_path, 'DBpedia_graph', entity_map_values_temp[:(-5)] + '.txt'), 'r')
            edge_temp = []
            while True:
                f_readline_temp = f.readline()
                if f_readline_temp != '':
                    edge_temp.append(copy.deepcopy(eval(f_readline_temp)))
                else:
                    break
            f.close()
            os.remove(os.path.join(self.DBpedia_path, 'DBpedia_graph', entity_map_values_temp[:(-5)] + '.txt'))

            f = open(os.path.join(self.DBpedia_path, 'DBpedia_graph', entity_map_values_temp), 'w')
            json.dump(obj=edge_temp, fp=f, indent=4)
            f.close()

            progress_bar.update(i + 1)
        print('\n')

        ### 保存self.entity_map
        f = open(os.path.join(self.DBpedia_path, 'DBpedia_graph', 'entity_map.json'), 'w')
        json.dump(obj=self.entity_map, fp=f, indent=4)
        f.close()

        ### 打包
        print('Transform dir to .tar')
        f = tarfile.open(os.path.join(self.DBpedia_path, 'DBpedia_graph.tar'), 'w')
        file_name = os.listdir(os.path.join(self.DBpedia_path, 'DBpedia_graph'))
        progress_bar = progressbar.ProgressBar(max_value=len(file_name))
        for i, file_name_temp in enumerate(file_name):
            f.add(os.path.join(self.DBpedia_path, 'DBpedia_graph', file_name_temp), arcname=file_name_temp)
            ### 删除打包前的各个文件
            os.remove(os.path.join(self.DBpedia_path, 'DBpedia_graph', file_name_temp))

            progress_bar.update(i + 1)
        print('\n')

        f.close()
        ### 删除临时目录
        os.removedirs(os.path.join(self.DBpedia_path, 'DBpedia_graph'))

    def write_DBpedia_graph(self, DBpedia_graph_temp):
        ### 保存数据
        print('Save DBpedia graph')
        ### progress_bar 是局部变量，不影响 generate_DBpedia_graph() 中的 progress_bar
        progress_bar = progressbar.ProgressBar(max_value=len(DBpedia_graph_temp))
        for i, (DBpedia_graph_temp_keys, DBpedia_graph_temp_values) in enumerate(DBpedia_graph_temp.items()):
            ### 如果文件不存在，选项 'a' 也会新建文件
            f = open(os.path.join(self.DBpedia_path, 'DBpedia_graph', self.entity_map[DBpedia_graph_temp_keys][:(-5)] + '.txt'), 'a')
            for DBpedia_graph_temp_values_temp in DBpedia_graph_temp_values:
                f.write(str(DBpedia_graph_temp_values_temp) + '\n')
            f.close()

            progress_bar.update(i + 1)
        print('\n')

    ### entity 的类型为 str
    ### 返回类型为 bool
    def is_existing(self, entity):
        return entity in self.entity_map_key_set

    ### entity 的类型为 str
    ### 返回类型为 list -> dict
    def get_edge(self, entity):
        ### 检验entity是否在DBpedia_graph中
        if self.is_existing(entity) == False:
            return None
        if entity not in self.edge_cache_key_set:
            ### 当该进程占用内存超过一定限额时，清除缓存，防止内存超限、程序终止
            ### 当前程序对应的进程不会改变，为了减少时间花费，只在类初始化创建一次
            #process = psutil.Process(os.getpid())
            ### process.memory_info().rss的单位是字节
            if self.process.memory_info().rss >= (self.memory_limit * (2**30)):
                print('Release the edge cache')
                self.clear_cache()

            self.edge_cache.update({entity: json.load(self.DBpedia_graph_file.extractfile(self.entity_map[entity]))})
            self.edge_cache_key_set.add(entity)

        return self.edge_cache[entity]

    ### entity 的类型为 str
    ### 返回类型为 set
    def get_endpoint_set(self, entity):
        ### 检验entity是否在DBpedia_graph中
        if self.is_existing(entity) == False:
            return None
        if entity not in self.endpoint_set_cache_key_set:
            self.endpoint_set_cache.update({entity: set()})
            for i, edge_temp in enumerate(self.get_edge(entity)):
                self.endpoint_set_cache[entity].add(edge_temp['endpoint'])
            self.endpoint_set_cache_key_set.add(entity)
        return self.endpoint_set_cache[entity]

    ### entity 和 endpoint 的类型为 str
    ### 返回类型为 list -> dict
    def endpoint_to_edge_list(self, entity, endpoint):
        ### 检验entity是否在DBpedia_graph中
        if self.is_existing(entity) == False:
            return None
        ### 检验endpoint是否与entity相连
        if endpoint in self.get_endpoint_set(entity):
            if entity not in self.endpoint_index_cache_key_set:
                self.endpoint_index_cache.update({entity: {}})
                for i, edge_temp in enumerate(self.get_edge(entity)):
                    ### 注意entity、endpoint固定，edge不一定唯一，可能谓语和方向不同！
                    if edge_temp['endpoint'] not in self.endpoint_index_cache[entity]:
                        self.endpoint_index_cache[entity].update({
                            edge_temp['endpoint']: []
                        })
                    self.endpoint_index_cache[entity][edge_temp['endpoint']].append(i)
                self.endpoint_index_cache_key_set.add(entity)
            edge_list_temp = []
            for i in self.endpoint_index_cache[entity][endpoint]:
                edge_list_temp.append(self.get_edge(entity)[i])
            return edge_list_temp
        else:
            return None

    def get_direction(self, entity, endpoint, predicate):
        ### 检验entity是否在DBpedia_graph中
        if self.is_existing(entity) == False:
            return None
        ### 检验endpoint是否与entity相连
        if endpoint in self.get_endpoint_set(entity):
            for edge_temp in self.endpoint_to_edge_list(entity, endpoint):
                if edge_temp['predicate'] == predicate:
                    return edge_temp['direction']
            else: ### endpoint与entity之间没有该predicate
                return None
        else:
            return None

    def get_reverse_direction(self, entity, endpoint, predicate):
        ### 检验entity是否在DBpedia_graph中
        if self.is_existing(entity) == False:
            return None
        ### 检验endpoint是否与entity相连
        if endpoint in self.get_endpoint_set(entity):
            for edge_temp in self.endpoint_to_edge_list(entity, endpoint):
                if edge_temp['predicate'] == predicate:
                    return 'out' if edge_temp['direction'] == 'in' else 'in'
            else: ### endpoint与entity之间没有该predicate
                return None
        else:
            return None

    def clear_cache(self):
        self.edge_cache.clear()
        self.edge_cache_key_set.clear()
        self.endpoint_set_cache.clear()
        self.endpoint_set_cache_key_set.clear()
        self.endpoint_index_cache.clear()
        self.endpoint_index_cache_key_set.clear()

import pickle
import copy
import progressbar
import jsonlines
def read_upstream_data(process_upstream_data, upstream_entity_data_path, upstream_core_inferential_chain_data_path, upstream_golden_core_inferential_chain_data_path):
    if process_upstream_data == True:
        entity_data = class_entity_data(upstream_entity_data_path)
        core_inferential_chain_data = class_core_inferential_chain_data(upstream_core_inferential_chain_data_path)
        core_inferential_chain_data.update(upstream_golden_core_inferential_chain_data_path)

        ### 保存数据
        f = open('class_entity_data.pkl', 'wb')
        pickle.dump(obj=entity_data, file=f)
        f.close()
        f = open('class_core_inferential_chain_data.pkl', 'wb')
        pickle.dump(obj=core_inferential_chain_data, file=f)
        f.close()
    
    else:
        ### 读取数据
        f = open('class_entity_data.pkl', 'rb')
        entity_data = pickle.load(f) 
        f.close()
        f = open('class_core_inferential_chain_data.pkl', 'rb')
        core_inferential_chain_data = pickle.load(f) 
        f.close()

    return entity_data, core_inferential_chain_data

class class_entity_data:
    def __init__(self, entity_data_path):
        f = open(entity_data_path, 'rb')
        raw_entity_data = pickle.load(f)
        f.close()

        print('Start processing upstream entity data')
        self.entity_data = []
        progress_bar = progressbar.ProgressBar(max_value=len(raw_entity_data))
        for i, (id_temp, raw_entity_data_temp) in enumerate(raw_entity_data.items()):
            entity_data_temp = {
                'id': copy.deepcopy(id_temp),
                'mention': {}
            }
            for search_result_keys_temp, search_result_values_temp in raw_entity_data_temp['search_result_per_keyword'].items():
                entity_data_temp['mention'].update({copy.deepcopy(search_result_keys_temp): []})
                for search_result_values_temp_temp in search_result_values_temp:
                    search_result_values_temp_temp_1_split = search_result_values_temp_temp[1].split()
                    for split_temp in search_result_values_temp_temp_1_split:
                        entity_data_temp['mention'][search_result_keys_temp].append({
                            'entity': copy.deepcopy(split_temp),
                            'string': copy.deepcopy(search_result_values_temp_temp[0]),
                            'score': copy.deepcopy(search_result_values_temp_temp[2])
                        })

            self.entity_data.append(copy.deepcopy(entity_data_temp))
            progress_bar.update(i + 1)
        print('\n')
        '''
        ### 为数据建立 id_to_index
        ### 默认1个id只对应1组entity_data
        self.id_to_index = {}
        for i, entity_data_temp in enumerate(self.entity_data):
            self.id_to_index.update({copy.deepcopy(entity_data_temp['id']): i})
        '''
        self.entity_to_string = {}
        for entity_data_temp in self.entity_data:
            self.entity_to_string.update({copy.deepcopy(entity_data_temp['id']): {}})
            for mention_keys_temp, mention_values_temp in entity_data_temp['mention'].items():
                self.entity_to_string[entity_data_temp['id']].update({copy.deepcopy(mention_keys_temp): {}})
                for mention_values_temp_temp in mention_values_temp:
                    self.entity_to_string[entity_data_temp['id']][mention_keys_temp].update({
                        copy.deepcopy(mention_values_temp_temp['entity']): copy.deepcopy(mention_values_temp_temp['string'])
                    })
    
    ### 返回类型为dict
    def get_entity_to_string_dict(self, id):
        return self.entity_to_string[id]

class class_core_inferential_chain_data:
    def __init__(self, core_inferential_chain_data_path):
        f = open(core_inferential_chain_data_path, 'r')
        f_reader = jsonlines.Reader(f)
        f_reader_length = 0
        for f_reader_temp in f_reader:
            f_reader_length += 1
        f.close()

        f = open(core_inferential_chain_data_path, 'r')
        f_reader = jsonlines.Reader(f)
        print('Start processing upstream core inferential chain data')
        self.core_inferential_chain_data = []
        '''
        每一种variable的取值都存储一个完整的candidate
        空间复杂度不变，N1 + N1*N2 + N1*N2*N3 -> 3*N1*N2*N3
        N1是topic entity的数量，N2是第1个variable的数量，N3是第2个variable的数量
        最终core inferential chain是1个list，第1个元素是topic entity的URI，之后元素依次为连接到每个variable的predicate的URI
        '''
        progress_bar = progressbar.ProgressBar(max_value=f_reader_length)
        ### f_reader_temp的类型是dict
        for i, f_reader_temp in enumerate(f_reader):
            ### 每1个question对应1个dict
            core_inferential_chain_data_temp = {
                'id': copy.deepcopy(f_reader_temp['id']),
                'candidate': []
            }
            for item_temp in f_reader_temp['items']:
                ### 1跳
                if len(item_temp['chain']) == 3:
                    if item_temp['chain'][0] == 'VAR':
                        topic_entity_temp = copy.deepcopy(item_temp['chain'][2])
                        core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][2]), copy.deepcopy(item_temp['chain'][1])]
                    else: ### item_temp['chain'][2] == 'VAR'
                        topic_entity_temp = copy.deepcopy(item_temp['chain'][0])
                        core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][0]), copy.deepcopy(item_temp['chain'][1])]
                    candidate_temp = {
                        'topic_entity': copy.deepcopy(topic_entity_temp), ### 'topic_entity'是str
                        'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp), ### 'core_inferential_chain'是list，第1个元素是topic entity，之后依次为predicate
                        'variable_value': [],
                        'score': copy.deepcopy(item_temp['pred'])
                    }
                    for var_values_temp in item_temp['var_values']:
                        candidate_temp['variable_value'].append([copy.deepcopy(var_values_temp)])
                    core_inferential_chain_data_temp['candidate'].append(copy.deepcopy(candidate_temp))
                ### 2跳
                elif len(item_temp['chain']) == 6:
                    ### item_temp['chain'][3]、item_temp['chain'][5]一定是变量，只需要谓语item_temp['chain'][4]
                    if item_temp['chain'][0] == 'VAR1':
                        topic_entity_temp = copy.deepcopy(item_temp['chain'][2])
                        core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][2]), copy.deepcopy(item_temp['chain'][1]), copy.deepcopy(item_temp['chain'][4])]
                    else: ### item_temp['chain'][2] == 'VAR1'
                        topic_entity_temp = copy.deepcopy(item_temp['chain'][0])
                        core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][0]), copy.deepcopy(item_temp['chain'][1]), copy.deepcopy(item_temp['chain'][4])]
                    candidate_temp = {
                        'topic_entity': copy.deepcopy(topic_entity_temp),
                        'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp),
                        'variable_value': [],
                        'score': copy.deepcopy(item_temp['pred'])
                    }
                    for var1_values_temp, var2_values_temp in item_temp['var_values'].items():
                        for var2_values_temp_temp in var2_values_temp:
                            candidate_temp['variable_value'].append([copy.deepcopy(var1_values_temp), copy.deepcopy(var2_values_temp_temp)])
                    core_inferential_chain_data_temp['candidate'].append(copy.deepcopy(candidate_temp))
                else:
                    print('chain 不是1、2跳')
                    continue ### 跳过这个 core inferential chain

            self.core_inferential_chain_data.append(copy.deepcopy(core_inferential_chain_data_temp))

            progress_bar.update(i + 1)
        print('\n')
        f.close()

        self.id_to_index = {}
        for i, core_inferential_chain_data_temp in enumerate(self.core_inferential_chain_data):
            self.id_to_index.update({copy.deepcopy(core_inferential_chain_data_temp['id']): i})

    ### 返回类型为 list -> dict
    def get_core_inferential_chain_list(self, id):
        return self.core_inferential_chain_data[self.id_to_index[id]]['candidate']

    ### 返回类型为 list，
    ### 这里使用in core_inferential_chain_data.get_id_list()和not in core_inferential_chain_data.get_id_list()做判断的次数不多
    ### 而且这里有序方便debug等
    def get_id_list(self):
        return list(self.id_to_index.keys())

    def update(self, core_inferential_chain_data_path):
        f = open(core_inferential_chain_data_path, 'r')
        f_reader = jsonlines.Reader(f)
        f_reader_length = 0
        for f_reader_temp in f_reader:
            f_reader_length += 1
        f.close()

        f = open(core_inferential_chain_data_path, 'r')
        f_reader = jsonlines.Reader(f)
        print('Start update upstream core inferential chain data')
        '''
        每一种variable的取值都存储一个完整的candidate
        空间复杂度不变，N1 + N1*N2 + N1*N2*N3 -> 3*N1*N2*N3
        N1是topic entity的数量，N2是第1个variable的数量，N3是第2个variable的数量
        最终core inferential chain是1个list，第1个元素是topic entity的URI，之后元素依次为连接到每个variable的predicate的URI
        '''
        progress_bar = progressbar.ProgressBar(max_value=f_reader_length)
        ### f_reader_temp的类型是dict
        for i, f_reader_temp in enumerate(f_reader):
            ### 初始化的数据里面不包含的question id不作处理
            if f_reader_temp['id'] in self.get_id_list():
                for item_temp in f_reader_temp['items']:
                    ### 1跳
                    if len(item_temp['chain']) == 3:
                        if item_temp['chain'][0] == 'VAR':
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][2])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][2]), copy.deepcopy(item_temp['chain'][1])]
                        else: ### item_temp['chain'][2] == 'VAR'
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][0])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][0]), copy.deepcopy(item_temp['chain'][1])]
                        candidate_temp = {
                            'topic_entity': copy.deepcopy(topic_entity_temp), ### 'topic_entity'是str
                            'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp), ### 'core_inferential_chain'是list，第1个元素是topic entity，之后依次为predicate
                            'variable_value': [],
                            'score': copy.deepcopy(item_temp['pred'])
                        }
                        for var_values_temp in item_temp['var_values']:
                            candidate_temp['variable_value'].append([copy.deepcopy(var_values_temp)])
                        self.core_inferential_chain_data[self.id_to_index[f_reader_temp['id']]]['candidate'].append(copy.deepcopy(candidate_temp))
                    ### 2跳
                    elif len(item_temp['chain']) == 6:
                        ### item_temp['chain'][3]、item_temp['chain'][5]一定是变量，只需要谓语item_temp['chain'][4]
                        if item_temp['chain'][0] == 'VAR1':
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][2])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][2]), copy.deepcopy(item_temp['chain'][1]), copy.deepcopy(item_temp['chain'][4])]
                        else: ### item_temp['chain'][2] == 'VAR1'
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][0])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][0]), copy.deepcopy(item_temp['chain'][1]), copy.deepcopy(item_temp['chain'][4])]
                        candidate_temp = {
                            'topic_entity': copy.deepcopy(topic_entity_temp),
                            'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp),
                            'variable_value': [],
                            'score': copy.deepcopy(item_temp['pred'])
                        }
                        for var1_values_temp, var2_values_temp in item_temp['var_values'].items():
                            for var2_values_temp_temp in var2_values_temp:
                                candidate_temp['variable_value'].append([copy.deepcopy(var1_values_temp), copy.deepcopy(var2_values_temp_temp)])
                        self.core_inferential_chain_data[self.id_to_index[f_reader_temp['id']]]['candidate'].append(copy.deepcopy(candidate_temp))
                    else:
                        print('chain 不是1、2跳')
                        continue ### 跳过这个 core inferential chain
            '''
            else:
                ### 每1个question对应1个dict
                core_inferential_chain_data_temp = {
                    'id': copy.deepcopy(f_reader_temp['id']),
                    'candidate': []
                }
                for item_temp in f_reader_temp['items']:
                    ### 1跳
                    if len(item_temp['chain']) == 3:
                        if item_temp['chain'][0] == 'VAR':
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][2])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][2]), copy.deepcopy(item_temp['chain'][1])]
                        else: ### item_temp['chain'][2] == 'VAR'
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][0])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][0]), copy.deepcopy(item_temp['chain'][1])]
                        candidate_temp = {
                            'topic_entity': copy.deepcopy(topic_entity_temp), ### 'topic_entity'是str
                            'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp), ### 'core_inferential_chain'是list，第1个元素是topic entity，之后依次为predicate
                            'variable_value': [],
                            'score': copy.deepcopy(item_temp['pred'])
                        }
                        for var_values_temp in item_temp['var_values']:
                            candidate_temp['variable_value'].append([copy.deepcopy(var_values_temp)])
                        core_inferential_chain_data_temp['candidate'].append(copy.deepcopy(candidate_temp))
                    ### 2跳
                    elif len(item_temp['chain']) == 6:
                        ### item_temp['chain'][3]、item_temp['chain'][5]一定是变量，只需要谓语item_temp['chain'][4]
                        if item_temp['chain'][0] == 'VAR1':
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][2])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][2]), copy.deepcopy(item_temp['chain'][1]), copy.deepcopy(item_temp['chain'][4])]
                        else: ### item_temp['chain'][2] == 'VAR1'
                            topic_entity_temp = copy.deepcopy(item_temp['chain'][0])
                            core_inferential_chain_temp = [copy.deepcopy(item_temp['chain'][0]), copy.deepcopy(item_temp['chain'][1]), copy.deepcopy(item_temp['chain'][4])]
                        candidate_temp = {
                            'topic_entity': copy.deepcopy(topic_entity_temp),
                            'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp),
                            'variable_value': [],
                            'score': copy.deepcopy(item_temp['pred'])
                        }
                        for var1_values_temp, var2_values_temp in item_temp['var_values'].items():
                            for var2_values_temp_temp in var2_values_temp:
                                candidate_temp['variable_value'].append([copy.deepcopy(var1_values_temp), copy.deepcopy(var2_values_temp_temp)])
                        core_inferential_chain_data_temp['candidate'].append(copy.deepcopy(candidate_temp))
                    else:
                        print('chain 不是1、2跳')
                        continue ### 跳过这个 core inferential chain

                self.core_inferential_chain_data.append(copy.deepcopy(core_inferential_chain_data_temp))
                self.id_to_index.update({copy.deepcopy(f_reader_temp['id']): len(self.core_inferential_chain_data)})
            '''

            progress_bar.update(i + 1)
        print('\n')
        f.close()

import copy
import itertools
import time
from scipy.special import comb
import math
import numpy as np
### max_computation_time用于限制每个core inferential chain可接受的最大计算时间，单位为秒。如果超过这个最大计算事件，则跳过该core inferential chain
def constraints_candidate_generation(make_candidate, QueryGraph, DBpedia_graph, entity_data, core_inferential_chain_data, max_computation_time=10, constraints_type=['triple']):
    if make_candidate == True:
        ### QueryGraph_temp 是引用
        ### 增加键 'query_graph_candidate'
        ### 为Query Graph建立id_to_index
        id_to_index_for_QueryGraph = {}
        for i, QueryGraph_temp in enumerate(QueryGraph):
            ### 如果结果里，'query_graph_candidate'字段等于None，说明上游entity缺失或上游core inferential chain缺失
            QueryGraph_temp.update({'query_graph_candidate': None})
            id_to_index_for_QueryGraph.update({copy.deepcopy(QueryGraph_temp['id']): i})

        print('Start generating constraints')
        '''
        每一个question（同一个'id'值）归为一类，称为1级类。
        每类内部，每个跳数（1跳、2跳）归为一类，称为2级类。
        每类内部，topic entity、predicate取值相同的归为一类，称为3级类。
        每类内部，variable的每一种取值，对应一个constraints集合。
        假设某个3级类内部有N种variable的取值，则对应N个constraints集合。
        那么这N个constraints集合的全部组合，每种组合内部取并集，共2^N种（最少0个集合求并集，最多N个集合求并集），就是该3级类对应的全部可能的constraints取值。
        这2^N种constraints取值对应该3级类的2^N个Query Graph candidate
        '''
        progress_bar = progressbar.ProgressBar(max_value=len(core_inferential_chain_data.get_id_list()))
        #process_number = 10 ### 为了实验效率，只处理process_number个question
        ### 遍历每个question
        for i, id_temp in enumerate(core_inferential_chain_data.get_id_list()):
            print('i: %d, id: %d' % (i, id_temp))
            ### 目前只处理query graph golden完整，并且constraints里面只有triple的question
            if skip(QueryGraph[id_to_index_for_QueryGraph[id_temp]], constraints_type) == True:
                continue
            
            ### 如果core_inferential_chain_data的question id在QuryGraph中不存在，就跳过该question
            if id_temp not in id_to_index_for_QueryGraph:
                continue
            '''
            ### 为了实验效率，只处理process_number个question
            else:
                process_number -= 1
                if process_number < 0:
                    break
            '''
            #print('entity_data.get_entity_to_string_dict(id_temp): ', entity_data.get_entity_to_string_dict(id_temp))
            ### 把None变为[]，表示存在
            QueryGraph[id_to_index_for_QueryGraph[id_temp]]['query_graph_candidate'] = []
            ### 遍历每种topic entity和core inferential chain
            for core_inferential_chain_temp in core_inferential_chain_data.get_core_inferential_chain_list(id_temp):
                print('\ni: %d, id: %d' % (i, id_temp))
                #print('core_inferential_chain_temp[\'core_inferential_chain\']: \n', core_inferential_chain_temp['core_inferential_chain'])
                #print('core_inferential_chain_temp[\'variable_value\']: \n', core_inferential_chain_temp['variable_value'])
                ### 每种core inferential chain内部进行constraints集合的组合取交集，因此需要建立constraints集合列表
                ### constraints_set的元素是某一种variable的取值的constraints集合
                constraints_set = [] 
                ### 遍历每种variable取值，求每种variable取值的constraints集合，结果是由集合组成的列表
                print('len(core_inferential_chain_temp[\'variable_value\']): ', len(core_inferential_chain_temp['variable_value']))
                variable_value_number = len(core_inferential_chain_temp['variable_value'])
                '''
                根据variable_value_number估计最大计算时间，并用max_computation_time限制。最大计算时间超过max_computation_time，则跳过该core inferential chain
                根据观察variable_value_number和用时的关系为
                110000 7秒
                20000 3秒
                20000 2秒
                50000 3秒
                40000 12秒
                30000 10秒
                10000 4秒
                因此估计variable_value_number:用时=60000:10
                '''
                if variable_value_number > max_computation_time * 6000:
                    print('The amount of computation is more than max_computation_time')
                    print('core inferential chain: ', core_inferential_chain_temp['core_inferential_chain'])
                    continue
                start_time = time.time()
                progress_bar1 = progressbar.ProgressBar(max_value=len(core_inferential_chain_temp['variable_value']))
                for p1, variable_value_temp in enumerate(core_inferential_chain_temp['variable_value']):
                    ### 三元组constraints，且constraints 'ArgumentType' 是Entity
                    triple_constraints_key = [
                        'type',
                        'Operator',
                        'Direction',
                        'ArgumentType',
                        'Argument',
                        'EntityName',
                        'SourceNodeIndex',
                        'NodePredicate',
                        'ValueType'
                    ]
                    constraints_set_for_each_mention = {}
                    ### constraints_set_for_each_mention -> mention -> list -> set，list里每个set对应一种该mention的一个entity string对
                    ### 遍历该question抽取的mention
                    for mention_temp, entity_to_string_temp in entity_data.get_entity_to_string_dict(id_temp).items():
                        ### 为每一个mention建立一个列表
                        constraints_set_for_each_mention.update({copy.deepcopy(mention_temp): []})
                        ### 遍历该mention对应的entity
                        ### 一个constraints candidate里面，一种mention里只能有一个entity出现
                        for entity_temp, string_temp in entity_to_string_temp.items():
                            ### 为该mention的每一个entity建立一个list -> set
                            constraints_set_for_each_entity = [set()]
                            ### 依次遍历core inferential chain上的variable
                            for j, variable_value_temp_temp in enumerate(variable_value_temp):
                                ### 如果variable_value_temp_temp不是Entity，则不设置constraints
                                if not ((variable_value_temp_temp[0] == '<') and (variable_value_temp_temp[-1] == '>')):
                                    continue
                                ### DBpedia_graph.get_endpoint_set() 和 DBpedia_graph.endpoint_to_edge_list() 的 entity参数 应该传入entity_data的entity，而不传入core_inferential_chain_data的variable的取值entity
                                ### 因为前者范围小、数量少，那么DBpedia_graph需要加载的edge文件少，需要建立的endpoint_set_cache、endpoint_index_cache也少；后者则相反
                                ### 如果entity_temp不在DBpedia_graph中，就跳过该entity_temp
                                if DBpedia_graph.is_existing(entity_temp) == False:
                                    continue
                                if variable_value_temp_temp in DBpedia_graph.get_endpoint_set(entity_temp):
                                    ### 不能用dict作为set的元素，因为多个键值对之间是无序的。元素相同，顺序不同无法处理
                                    ### 这里要使用list，防止出BUG。集合的元素为list -> list，最里层有2个list，第1个为值，第2个为键
                                    edge_list = DBpedia_graph.endpoint_to_edge_list(entity_temp, variable_value_temp_temp)
                                    ### 遍历entity为entity_temp，endpoint为variable_value_temp_temp的所有边
                                    ### 一个constraints candidate里，在一个variable位置上，一对entity、endpoint之间可能有多种predicate，对这多种predicate进行组合
                                    constraints_set_list = []
                                    for edge_list_temp in edge_list:
                                        constraints_set_list.append({str([[
                                            'triple', ### 'type':  
                                            'Equal', ### 'Operator': 
                                            DBpedia_graph.get_reverse_direction(entity_temp, variable_value_temp_temp, edge_list_temp['predicate']), ### 'Direction': 对'Direction'进行翻转，实际上是 'out' if edge_list_temp['direction'] == 'in' else 'in',
                                            'Entity', ### 'ArgumentType': 
                                            copy.deepcopy(entity_temp), ### 'Argument': 
                                            copy.deepcopy(string_temp), ### 'EntityName': 
                                            j, ### 'SourceNodeIndex': 
                                            copy.deepcopy(edge_list_temp['predicate']), ### 'NodePredicate': 
                                            None ### 'ValueType': 
                                        ], copy.deepcopy(triple_constraints_key)])})
                                        '''
                                        print([
                                            'triple', ### 'type':  
                                            'Equal', ### 'Operator': 
                                            'out' if edge_list_temp['direction'] == 'in' else 'in', ### 'Direction': ，对'Direction'进行翻转
                                            'Entity', ### 'ArgumentType': 
                                            copy.deepcopy(entity_temp), ### 'Argument': 
                                            copy.deepcopy(string_temp), ### 'EntityName': 
                                            j, ### 'SourceNodeIndex': 
                                            copy.deepcopy(edge_list_temp['predicate']), ### 'NodePredicate':
                                            None ### 'ValueType': 
                                            ])
                                        '''
                                    constraints_set_list_combination_union = []
                                    for k in range(1, len(constraints_set_list) + 1):
                                        for constraints_set_list_combination_temp in itertools.combinations(constraints_set_list, k):
                                            constraints_set_list_combination_union.append(set().union(*constraints_set_list_combination_temp))
                                    
                                    constraints_set_for_each_entity_temp = []
                                    ### 遍历当前积累的所有constraints set
                                    for constraints_set_for_each_entity_temp_temp in constraints_set_for_each_entity:
                                        ### 遍历新的constraints set
                                        for constraints_set_list_combination_union_temp in constraints_set_list_combination_union:
                                            constraints_set_for_each_entity_temp.append(constraints_set_for_each_entity_temp_temp.union(constraints_set_list_combination_union_temp))
                                    if len(constraints_set_for_each_entity_temp) > 0:
                                        constraints_set_for_each_entity = constraints_set_for_each_entity_temp

                            ### 更新的同时实现去除空集和去重
                            for constraints_set_for_each_entity_temp in constraints_set_for_each_entity:
                                if (len(constraints_set_for_each_entity_temp) > 0) and \
                                    (constraints_set_for_each_entity_temp not in constraints_set_for_each_mention[mention_temp]):
                                    constraints_set_for_each_mention[mention_temp].append(copy.deepcopy(constraints_set_for_each_entity_temp))
                    '''
                    ### 测试用，如果用这段代码替换下面这段代码，速度不变，则说明速度瓶颈不在不同mention的组合计算上
                    constraints_set.append([])
                    for mention_temp, constraints_set_for_each_mention_temp in constraints_set_for_each_mention.items():
                            for constraints_set_for_each_mention_temp_temp in constraints_set_for_each_mention_temp:
                                constraints_set[-1].append(copy.deepcopy(constraints_set_for_each_mention_temp_temp))
                    '''
                    constraints_set_mention_combination = [set()]
                    ### 遍历每种mention
                    for mention_temp, constraints_set_for_each_mention_temp in constraints_set_for_each_mention.items():
                        constraints_set_mention_combination_temp = []
                        ### 遍历当前积累的所有constraints set
                        for constraints_set_mention_combination_temp_temp in constraints_set_mention_combination:
                            ### 遍历每种mention的所有constraints set（每个constraints set对应1个entity）
                            for constraints_set_for_each_mention_temp_temp in constraints_set_for_each_mention_temp:
                                constraints_set_mention_combination_temp_temp_copy = copy.deepcopy(constraints_set_mention_combination_temp_temp)
                                ### 求并集
                                constraints_set_mention_combination_temp_temp_copy.update(copy.deepcopy(constraints_set_for_each_mention_temp_temp))
                                ### 更新的同时实现去重。注意，对于每个mention，如果只有空集，则保留空集；否则，如果有非空集，则去掉空集
                                if (len(constraints_set_mention_combination_temp_temp_copy) != 0) and \
                                    (constraints_set_mention_combination_temp_temp_copy not in constraints_set_mention_combination_temp):
                                    constraints_set_mention_combination_temp.append(constraints_set_mention_combination_temp_temp_copy)
                        ### len(constraints_set_for_each_mention_temp)>0，且有非空集合
                        if len(constraints_set_mention_combination_temp) > 0:
                            constraints_set_mention_combination = constraints_set_mention_combination_temp

                    ### 把一种core inferential chain的一种variable取值的多个constraints set单独存为1个列表
                    constraints_set_temp = []
                    ### 更新的同时实现去除空集和去重
                    for constraints_set_mention_combination_temp in constraints_set_mention_combination:
                        if (len(constraints_set_mention_combination_temp) != 0) and \
                            constraints_set_mention_combination_temp not in constraints_set_temp:
                            constraints_set_temp.append(copy.deepcopy(constraints_set_mention_combination_temp))
                    constraints_set.append(copy.deepcopy(constraints_set_temp))

                    progress_bar1.update(p1 + 1)
                used_time = time.time() - start_time
                if used_time > 1:
                    print('---------------------------------------------所有variable value处理用时（秒）：%.2f' % (used_time))

                ### 当len(constraints_set)等于0的时候，constraints_set_average_number_per_variable_value等于0
                constraints_set_average_number_per_variable_value = 0
                for constraints_set_temp in constraints_set:
                    constraints_set_average_number_per_variable_value += len(constraints_set_temp) / len(constraints_set)
                print('constraints_set_average_number_per_variable_value: %.2f' % (constraints_set_average_number_per_variable_value))
                '''
                某一个core inferential chain估计的最大计算量为 (variable_value_number ^ max_subset) * (constraints_set_average_number_per_variable_value ^ max_subset)
                根据观察，若计算量为2*10^7，用时为3分钟；若计算量为1*10^9，用时为120分钟。也就是说计算量的数量级和用时的数量级近似为正比关系
                因此，默认值max_computation_time=10一般对应最大计算量10^6
                防止constraints_set_average_number_per_variable_value过小，导致计算量估计过小，可以用max(constraints_set_average_number_per_variable_value, 1)代替
                '''
                if variable_value_number * max(constraints_set_average_number_per_variable_value, 1) >= max_computation_time * (10 ** 5):
                    print('The amount of computation is more than max_computation_time')
                    print('core inferential chain: ', core_inferential_chain_temp['core_inferential_chain'])
                    continue

                '''
                ### 对constraints集合去重
                constraints_set_after_removing_duplicate = []
                for constraints_set_temp in constraints_set:
                    if constraints_set_temp not in constraints_set_after_removing_duplicate:
                        constraints_set_after_removing_duplicate.append(copy.deepcopy(constraints_set_temp))
                constraints_set = constraints_set_after_removing_duplicate
                '''

                ### 对variable value求并集等价于对constraints set组合求交集，结果是交集（集合）组成的列表
                ### 每一种组合里，每一种variable取值的constraints set最多只能有1个。如果每一种variable取值有多个constraints set（每个constraints set对应1个mention entity组合），需要进行组合
                ### 因此首先对variable的取值进行组合。在每一种组合内部，每种variable的取值每次只取1个constraints set，进行组合
                ### 下面这种写法适用于max_subset比较小的情况（max_subset <= 4）
                ### j = 0 就是constraints为空集。list(itertools.combinations([], 0)) 返回 [()]
                ### j = len(constraints_set) 就是所有constraints集合求交集
                ### max_subset 是组合中，取constraints集合的最大数量
                constraints_set_combinations_intersection = [] ### 元素是set类型
                # max_subset = 2
                '''
                根据 max_computation_time、variable_value_number、constraints_set_average_number_per_variable_value，动态计算 max_subset
                某一个core inferential chain估计的最大计算量为 (variable_value_number ^ max_subset) * (constraints_set_average_number_per_variable_value ^ max_subset)
                根据观察，若计算量为2*10^7，用时为3分钟；若计算量为1*10^9，用时为120分钟。也就是说计算量的数量级和用时的数量级近似为正比关系
                因此，默认值max_computation_time=10一般对应最大计算量10^6
                防止constraints_set_average_number_per_variable_value过小，导致计算量估计过小，可以用max(constraints_set_average_number_per_variable_value, 1)代替
                '''
                eps = 10**(-8)
                max_subset = int(math.log(max_computation_time * (10 ** 5) + eps, variable_value_number * max(constraints_set_average_number_per_variable_value, 1) + eps))
                for j in range(0, min(len(constraints_set) + 1, max_subset + 1)):
                    ### 固定j，遍历所有组合
                    print('comb(len(constraints_set), %d): %d' % (j, comb(len(constraints_set), j)))
                    progress_bar2 = progressbar.ProgressBar(max_value=comb(len(constraints_set), j))
                    for p2, constraints_set_combinations_temp in enumerate(itertools.combinations(constraints_set, j)):
                        ### constraints_set_combinations_intersection_temp存储一种组合的constraints set，子集大小为j
                        constraints_set_combinations_intersection_temp = [set()]
                        ### 遍历一种组合的每个元素，一个元素对应一种variable取值
                        for k, constraints_set_combinations_temp_temp in enumerate(constraints_set_combinations_temp):
                            constraints_set_combinations_intersection_temp_temp = []
                            ### 遍历当前积累的所有constraints set
                            for constraints_set_combinations_intersection_temp_temp_temp in constraints_set_combinations_intersection_temp:
                                ### 遍历该variable取值的所有constraints set
                                for constraints_set_combinations_temp_temp_temp in constraints_set_combinations_temp_temp:
                                    if k == 0:
                                        constraints_set_combinations_intersection_temp_temp.append(
                                            copy.deepcopy(constraints_set_combinations_intersection_temp_temp_temp.union(constraints_set_combinations_temp_temp_temp))
                                        )
                                    else:
                                        constraints_set_combinations_intersection_temp_temp.append(
                                            copy.deepcopy(constraints_set_combinations_intersection_temp_temp_temp.intersection(constraints_set_combinations_temp_temp_temp))
                                        )
                            if len(constraints_set_combinations_intersection_temp_temp) > 0:
                                constraints_set_combinations_intersection_temp = constraints_set_combinations_intersection_temp_temp

                        ### 更新一种组合的constraints set，子集大小为j
                        ### 更新的同时实现去重，这里不去除空集
                        for constraints_set_combinations_intersection_temp_temp in constraints_set_combinations_intersection_temp:
                            if constraints_set_combinations_intersection_temp_temp not in constraints_set_combinations_intersection:
                                constraints_set_combinations_intersection.append(copy.deepcopy(constraints_set_combinations_intersection_temp_temp))

                        progress_bar2.update(p2 + 1)
                
                '''
                ### 对交集去重
                constraints_set_combinations_intersection_after_removing_duplicate = []
                for constraints_set_combinations_intersection_temp in constraints_set_combinations_intersection:
                    if constraints_set_combinations_intersection_temp not in constraints_set_combinations_intersection_after_removing_duplicate:
                        constraints_set_combinations_intersection_after_removing_duplicate.append(copy.deepcopy(constraints_set_combinations_intersection_temp))
                constraints_set_combinations_intersection = constraints_set_combinations_intersection_after_removing_duplicate
                '''

                ### 每个交集对应一种constraints candidate，从而生成一种Query Graph candidate
                print('len(constraints_set_combinations_intersection): ', len(constraints_set_combinations_intersection))
                progress_bar3 = progressbar.ProgressBar(max_value=len(constraints_set_combinations_intersection))
                for p3, constraints_set_combinations_intersection_temp in enumerate(constraints_set_combinations_intersection):
                    constraints_temp = []
                    while len(constraints_set_combinations_intersection_temp) > 0:
                        constraints_set_combinations_intersection_temp_list = copy.deepcopy(eval(constraints_set_combinations_intersection_temp.pop()))
                        constraints_set_combinations_intersection_temp_dict = {}
                        for j in range(len(constraints_set_combinations_intersection_temp_list[0])):
                            constraints_set_combinations_intersection_temp_dict.update({
                                copy.deepcopy(constraints_set_combinations_intersection_temp_list[1][j]): copy.deepcopy(constraints_set_combinations_intersection_temp_list[0][j])
                            })
                        constraints_temp.append(copy.deepcopy(constraints_set_combinations_intersection_temp_dict))
                    query_graph_candidate_temp = {
                        'topic_entity': copy.deepcopy(core_inferential_chain_temp['topic_entity']),
                        'core_inferential_chain': copy.deepcopy(core_inferential_chain_temp['core_inferential_chain']),
                        'constraints': copy.deepcopy(constraints_temp),
                        'label': None
                    }
                    ### 更新的同时实现去重
                    if query_graph_candidate_temp not in QueryGraph[id_to_index_for_QueryGraph[id_temp]]['query_graph_candidate']:
                        QueryGraph[id_to_index_for_QueryGraph[id_temp]]['query_graph_candidate'].append(copy.deepcopy(query_graph_candidate_temp))
                    
                    progress_bar3.update(p3 + 1)

            progress_bar.update(i + 1)
        print('\n')

        f = open('data_with_constraints_candidate.json', 'w')
        json.dump(obj=QueryGraph, fp=f, indent=4)
        f.close()
    
    else:
        f = open('data_with_constraints_candidate.json', 'r')
        QueryGraph = json.load(f) ### QueryGraph为dict->list类型
        f.close()
    
    return QueryGraph

### single_QueryGraph的类型为dict
### constraints_type的类型为list -> str
def skip(single_QueryGraph, constraints_type):
    if single_QueryGraph['query_graph_golden'][0]['topic_entity'] == None:
        return True
    if single_QueryGraph['query_graph_golden'][0]['core_inferential_chain'] == None:
        return True
    if single_QueryGraph['query_graph_golden'][0]['constraints'] == None:
        return True
    for query_graph_golden_temp in single_QueryGraph['query_graph_golden']:
        if (query_graph_golden_temp['topic_entity'] != None) and \
            (query_graph_golden_temp['core_inferential_chain'] != None) and \
            (query_graph_golden_temp['constraints'] != None):
            for constraints_temp in query_graph_golden_temp['constraints']:
                if constraints_temp['type'] not in constraints_type:
                    break
                elif constraints_temp['ArgumentType'] != 'Entity':
                    break
            else:
                return False
    else:
        return True

def generate_label(make_label, QueryGraph):
    if make_label == True:
        ### QueryGraph_temp 是引用
        ### 添加'available_for_model'键
        for QueryGraph_temp in QueryGraph:
            QueryGraph_temp.update({'available_for_model': None})

        for QueryGraph_temp in QueryGraph:
            ### 'available_for_model' 为 True，则可以被用于训练或测试
            ### 'available_for_model' 为 str，则不可以被用于训练或测试，并且str是原因
            unavailable_reason = ''
            ### 第1个'query_graph_golden'一定是最先搜索到的、最完整的query graph
            if QueryGraph_temp['query_graph_golden'][0]['topic_entity'] == None:
                unavailable_reason += 'golden topic_entity is None;'
            if QueryGraph_temp['query_graph_golden'][0]['core_inferential_chain'] == None:
                unavailable_reason += 'golden core_inferential_chain is None;'
            if QueryGraph_temp['query_graph_golden'][0]['constraints'] == None:
                unavailable_reason += 'golden constraints is None;'
            if QueryGraph_temp['query_graph_candidate'] == None:
                unavailable_reason += 'query_graph_candidate is None;'
            if unavailable_reason == '':
                QueryGraph_temp['available_for_model'] = True
            else:
                QueryGraph_temp['available_for_model'] = unavailable_reason
                continue

            ### 统计golden query graph的URI
            ### query_graph_golden_content的类型是list -> list，为每个query graph candidate单独建立1个list
            ### query_graph_golden_content包含在主语、谓语、宾语上的entity、literal等
            query_graph_golden_content = []
            for query_graph_golden_temp in QueryGraph_temp['query_graph_golden']:
                if (query_graph_golden_temp['topic_entity'] != None) and \
                    (query_graph_golden_temp['core_inferential_chain'] != None) and \
                    (query_graph_golden_temp['constraints'] != None):
                    query_graph_golden_content.append(set())
                    query_graph_golden_content[-1].add(copy.deepcopy(query_graph_golden_temp['topic_entity']))
                    for core_inferential_chain_temp in query_graph_golden_temp['core_inferential_chain']:
                        query_graph_golden_content[-1].add(copy.deepcopy(core_inferential_chain_temp['predicate']))
                    for constraints_temp in query_graph_golden_temp['constraints']:
                        query_graph_golden_content[-1].add(copy.deepcopy(constraints_temp['Argument']))
                        query_graph_golden_content[-1].add(copy.deepcopy(constraints_temp['NodePredicate']))
            
            ### query_graph_candidate_temp是引用
            for query_graph_candidate_temp in QueryGraph_temp['query_graph_candidate']:
                query_graph_candidate_content = set()
                query_graph_candidate_content.add(copy.deepcopy(query_graph_candidate_temp['topic_entity']))
                for core_inferential_chain_temp in query_graph_candidate_temp['core_inferential_chain'][1:]:
                    query_graph_candidate_content.add(copy.deepcopy(core_inferential_chain_temp))
                for constraints_temp in query_graph_candidate_temp['constraints']:
                    query_graph_candidate_content.add(copy.deepcopy(constraints_temp['Argument']))
                    query_graph_candidate_content.add(copy.deepcopy(constraints_temp['NodePredicate']))
                if query_graph_candidate_content in query_graph_golden_content:
                    query_graph_candidate_temp['label'] = 1
                else:
                    query_graph_candidate_temp['label'] = 0

        f = open('data_with_label.json', 'w')
        json.dump(obj=QueryGraph, fp=f, indent=4)
        f.close()
    
    else:
        f = open('data_with_label.json', 'r')
        QueryGraph = json.load(f) ### QueryGraph为dict->list类型
        f.close()
    
    return QueryGraph

import numpy as np
def statistical_result(QueryGraph):
    available_question_number = 0
    query_graph_candidate_number_per_question = []
    positive_sample_ratio_per_question = []
    constraints_candidate_number_per_query_graph_candidate = []
    for QueryGraph_temp in QueryGraph:
        if QueryGraph_temp['available_for_model'] == True:
            available_question_number += 1
            query_graph_candidate_number_per_question.append(len(QueryGraph_temp['query_graph_candidate']))
            positive_sample_ratio_per_question_temp = 0
            for query_graph_candidate_temp in QueryGraph_temp['query_graph_candidate']:
                constraints_candidate_number_per_query_graph_candidate.append(len(query_graph_candidate_temp['constraints']))
                positive_sample_ratio_per_question_temp += query_graph_candidate_temp['label'] / len(QueryGraph_temp['query_graph_candidate'])
            positive_sample_ratio_per_question.append(positive_sample_ratio_per_question_temp)
    print('available_question_number: %d' % (available_question_number))
    print('mean std min max')
    print('query_graph_candidate_number_per_question: %.2f %.2f %.2f %.2f' % (
        np.mean(query_graph_candidate_number_per_question),
        np.std(query_graph_candidate_number_per_question),
        min(query_graph_candidate_number_per_question),
        max(query_graph_candidate_number_per_question)
    ))
    print('positive_sample_ratio_per_question: %.4f %.4f %.4f %.4f' % (
        np.mean(positive_sample_ratio_per_question),
        np.std(positive_sample_ratio_per_question),
        min(positive_sample_ratio_per_question),
        max(positive_sample_ratio_per_question)
    ))
    print('constraints_candidate_number_per_query_graph_candidate: %.2f %.2f %.2f %.2f' % (
        np.mean(constraints_candidate_number_per_query_graph_candidate),
        np.std(constraints_candidate_number_per_query_graph_candidate),
        min(constraints_candidate_number_per_query_graph_candidate),
        max(constraints_candidate_number_per_query_graph_candidate)
    ))
    

if __name__=="__main__":
    make_graph = False
    process_upstream_data = False
    upstream_entity_data_path = 'data/train.en.zeroshotpred.BM25.top10.pk'
    upstream_core_inferential_chain_data_path = 'data/train_data_all.jsonl'
    upstream_golden_core_inferential_chain_data_path = 'data/train_golden_cc.jsonl'
    make_candidate = True
    make_label = True

    QueryGraph = read_QueryGraph()
    DBpedia_graph = class_DBpedia_graph(make_graph)
    entity_data, core_inferential_chain_data = read_upstream_data(process_upstream_data, upstream_entity_data_path, upstream_core_inferential_chain_data_path, upstream_golden_core_inferential_chain_data_path)
    QueryGraph = constraints_candidate_generation(make_candidate, QueryGraph, DBpedia_graph, entity_data, core_inferential_chain_data)
    QueryGraph = generate_label(make_label, QueryGraph)
    statistical_result(QueryGraph)