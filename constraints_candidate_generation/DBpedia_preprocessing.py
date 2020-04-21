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
