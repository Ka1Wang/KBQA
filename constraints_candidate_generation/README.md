# constraints candidate generation
## 1. DBpedia_preprocessing.py 的说明
### 1.1. 包列表
```
os
sys
copy
json
progressbar
psutil
tarfile
```
### 1.2. 类初始化
（1）类初始化参数`make_graph`，类型`bool`。若之前未做预处理，则`make_graph=True`；若之前已做预处理，已生成`DBpedia_graph.tar`，则`make_graph=False`。<br>
（2）类初始化参数`DBpedia_path`，类型`str`。存放`.ttl`文件的目录。<br>
（3）类初始化参数`memory_limit`，类型`int`。该进程允许使用的最大内存，单位为GB。<br>
（4）类初始化参数`file_name_list`，类型`list`，元素类型`str`。要使用的`.ttl`文件名列表。<br>
（5）初始化结束后，在`DBpedia_path`目录下会生成`DBpedia_graph.tar`。
### 1.3. 类的使用
（1）方法`get_edge(entity)`。参数entity的类型为`str`，是要查询的entity的URI。返回1个`list`，包含该entity的所有边。元素的类型是`dict`，说明如下
```
{
  'direction': 'out' if 该entity是subject, or 'in' if 该entity是object
  'predicate': predicate的URI
  'endpoint': 另一端entity的URI
}
```
