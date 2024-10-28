import plotly.graph_objects as go
import pandas as pd
from collections import defaultdict
import regex as re
from datetime import datetime
import os
from icecream import ic

from sankey_settings import Settings


class SankeyMaker():
    # underlying logic
    def __src_tar_trans_data(self, source_col, target_col):
        df2 = self.df.groupby([source_col, target_col], as_index=0).agg(vals = (self.val_col, self.val_agg))
        df2[f"{source_col}_sum"] = df2.groupby(source_col)['vals'].transform("sum")
        df2[f"{target_col}_sum"] = df2.groupby(target_col)['vals'].transform("sum")
        
        total_vals = df2['vals'].sum()

        # get percentage on node
        df2[f'%{source_col}'] = df2[f'{source_col}_sum'].apply(lambda x: x * 100/total_vals)
        df2[f'%{target_col}'] = df2[f"{target_col}_sum"].apply(lambda x: x * 100/total_vals)
        
        # get percentage on link
        df2[f"%{target_col}/{source_col}"] = round((df2['vals'] / df2[f"{source_col}_sum"]) * 100)
        
        return df2
    
    def __src_tar_loop_data_1st(self, df2, source_col, target_col):
        _split_char = self.settings['split_char']
        
        for _, row in df2.iterrows():
            # Add source, target, value
            self.sankey_input['sources'].append(self.label_map[row[source_col]])
            self.sankey_input['targets'].append(self.label_map[row[target_col]])
            self.sankey_input['values'].append(row['vals'])
            
            # 1. first col
            # Change displayed labels
            _percen_on_node = round(row[f'%{source_col}'], 2)
            _sum_on_node = row[f"{source_col}_sum"]
            _percen_on_link = round(row[f"%{target_col}/{source_col}"], 2)
            _node_str_name = row[source_col]
            _node_index = self.label_map[_node_str_name]
            
            display_label = "" if _percen_on_node < Settings.SHOW_THRESHOLD else f"({str(_percen_on_node)}%, {str(_sum_on_node)})"
            self.sankey_input["display_labels"][_node_index] = f"{_node_str_name.split(_split_char)[-1]} {display_label}"
            
            self.sankey_input["stage_nodes_value"][0][_node_index] = _sum_on_node
            
            # Add node color
            node_color = self.color_node_map[_node_str_name.split("_")[-1]]
            self.sankey_input['nodes_color'][_node_index] = node_color
            
            
            # 2. second col
            # Change displayed labels
            _percen_on_node = round(row[f'%{target_col}'], 2)
            _sum_on_node = row[f"{target_col}_sum"]
            _node_str_name = row[target_col]
            _node_index = self.label_map[_node_str_name]
            
            display_label = "" if _percen_on_node < Settings.SHOW_THRESHOLD else f"({str(_percen_on_node)}%, {str(_sum_on_node)})"
            self.sankey_input["display_labels"][_node_index] = f"{_node_str_name.split(_split_char)[-1]} {display_label}"
            
            # Add custom data
            self.sankey_input["links_percent"].append(_percen_on_link)
            
            self.sankey_input["stage_nodes_value"][1][_node_index] = _sum_on_node
            
            # Addd node color
            node_color = self.color_node_map[_node_str_name.split("_")[-1]]
            self.sankey_input['nodes_color'][_node_index] = node_color
    
    def __src_tar_loop_data_not1st(self, df2, source_col, target_col, stage_counter):
        _split_char = self.settings['split_char']
        
        for _, row in df2.iterrows():
            if row[source_col].split(_split_char)[-1] == self.settings['node_tohide_sr_tar'] and row[target_col].split(_split_char)[-1] == self.settings['node_tohide_sr_tar']:
                continue
            
            # Add source, target, value
            self.sankey_input['sources'].append(self.label_map[row[source_col]])
            self.sankey_input['targets'].append(self.label_map[row[target_col]])
            self.sankey_input['values'].append(row['vals'])
            # 2. second col
            # Change displayed labels
            _percen_on_node = round(row[f'%{target_col}'], 2)
            _percen_on_link = round(row[f"%{target_col}/{source_col}"], 2)
            _sum_on_node = row[f"{target_col}_sum"]
            _node_str_name = row[target_col]
            _node_index = self.label_map[_node_str_name]
            
            display_label = "" if _percen_on_node < Settings.SHOW_THRESHOLD else f"({str(_percen_on_node)}%, {str(_sum_on_node)})"
            self.sankey_input["display_labels"][_node_index] = f"{_node_str_name.split(_split_char)[-1]} {display_label}"
            
            # Add custom data
            self.sankey_input["links_percent"].append(_percen_on_link)

            # Stage of display labels
            self.sankey_input["stage_nodes_value"][stage_counter][_node_index] = _sum_on_node
            
            # Addd node color
            node_color = self.color_node_map[_node_str_name.split("_")[-1]]
            self.sankey_input['nodes_color'][_node_index] = node_color
                
    def __src_tar_process(self, source_col, target_col, stage_counter = None, f_first_pair = 0):
        if f_first_pair:
            df2 = self.__src_tar_trans_data(source_col, target_col)
            self.__src_tar_loop_data_1st(df2, source_col, target_col)
        else:
            df2 = self.__src_tar_trans_data(source_col, target_col)
            self.__src_tar_loop_data_not1st(df2, source_col, target_col, stage_counter)

    def __set_color_theme(self):
        self.color_node_map = {}
        chose_theme = Settings.COLOR_THEME[self.settings['color_theme_name']]
        chose_theme.reverse()
        
        for node in self.unique_nodes_bef_rename:
            if node not in self.color_node_map:
                self.color_node_map[node] = chose_theme.pop()
                
        for node_overwrite in self.settings['color_overwrite']:
            if node_overwrite in self.color_node_map:
                self.color_node_map[node_overwrite] = self.settings['color_overwrite'][node_overwrite]


    # private execution steps
    def _read_data(self):
        self.df = pd.read_csv(self.input_data_path)
    
    def _rename_nodes(self):
        df = self.df.copy() 
        
        for stage_col, stage_name in self.stage_cols_map.items():
            df[stage_col] = df[stage_col].apply(lambda x: f"{stage_name}{Settings.STAGE_NAME_DELIMITER}{x}") # e.g. "STAGE1_CD"

        # get unique nodes
        self.unique_nodes_bef_rename = list(pd.concat([self.df[stage_col] for stage_col in self.stage_cols_map]).unique())
        self.unique_nodes_aft_rename = list(pd.concat([df[stage_col] for stage_col in self.stage_cols_map]).unique())
        self.label_map = {channel: idx for idx, channel in enumerate(self.unique_nodes_aft_rename)}
        
        # update df
        self.df = df
        
    def _prepare_custom_settings(self):
        self.settings = {
            "node_order": None,
            "color_theme_name": "THEME_1",
            "color_overwrite": {},
            "node_tohide_sr_tar": None,
            "split_char": Settings.STAGE_NAME_DELIMITER
        }
        
        if self.custom_settings is not None:
            for setting_name, setting_val in self.custom_settings.items():
                self.settings[setting_name] = setting_val 
    
    def _set_sankey_input(self):
        self.sankey_input = {
            "display_labels" : {},
            "sources" : [],
            "targets" : [],
            "values" : [],
            "links_percent" : [],
            "nodes_color": {},
            "links_color": [],
            "show_threshold" : Settings.SHOW_THRESHOLD,
            "color_major_threshold": {"node": 20, "link": 40},
            "stage_nodes_value": defaultdict(lambda: defaultdict(dict))
        } 
        
        # prepare color theme
        self.__set_color_theme()
        
        # trans data first source-target pair
        stage_cols_list = list(self.stage_cols_map.keys())
        
        source_col = stage_cols_list[0]
        target_col = stage_cols_list[1]
        self.__src_tar_process(source_col, target_col, f_first_pair = 1)
        
        # counter for stage of display labels
        stage_counter = 2
        
        ######### 2. PERFORM ON OTHERS SOURCE-TARGET PAIR
        if len(stage_cols_list) <= 2: # if out of index
            return self.sankey_input
        
        for i, node_col in enumerate(stage_cols_list[1:]):
            if i == len(stage_cols_list[1:]) - 1: # if out of index
                return self.sankey_input
            
            source_col = stage_cols_list[1:][i]
            target_col = stage_cols_list[1:][i + 1]
            self.__src_tar_process(source_col, target_col, stage_counter = stage_counter)
            stage_counter+=1
    
    def _set_stage_names(self):
        self.annotations = []
        
        stage_names_list = list(self.stage_cols_map.keys())
        
        for istage, stage_name  in enumerate(stage_names_list):
            self.annotations.append(dict(
                x = 0.001 + istage/(len(stage_names_list)-1),  # Position on the x-axis
                y = -0.03,  # Position on the y-axis
                xref='paper',  # Reference to the paper coordinates, not the data
                yref='paper',  # Reference to the paper coordinates
                text=self.stage_cols_map[stage_name],  # Text you want to display
                showarrow=False,  # Whether to show an arrow pointing to the annotation
                font=dict(size=15, color="#686D76", family="Arial"),  # Font properties
                align="center"  # Text alignment
            ))
        
    def _set_sankey_node_order(self):
        if self.settings['node_order'] is None: 
            self.sankey_input["sankey_node_order"] = None 
            return None 
        else:
            def _cal_x_axis(total_stage_num, cur_stage):
                x_axises = [0.001 + stage * 0.999/(total_stage_num - 1) for stage in range(total_stage_num)]
                return x_axises[cur_stage] if x_axises[cur_stage] != 1 else 0.999
                
            display_labels = self.sankey_input['display_labels']
            stage_nodes_value = self.sankey_input['stage_nodes_value']
            sankey_node_order = {
                "x_axises": {},
                "y_axises": {}
            }
            
            # calculate xaxis
            for _node_index, _node_name in display_labels.items():
                # get cur_Stage
                cur_stage = None
                for stage in stage_nodes_value:
                    for node in stage_nodes_value[stage]:
                        if node == _node_index:
                            cur_stage = stage    
                # get x values
                sankey_node_order["x_axises"][_node_index] = _cal_x_axis(total_stage_num = len(stage_nodes_value), cur_stage = cur_stage)
                
            # calculate y values
            for istage, stage in stage_nodes_value.items():
                last_node_values = []
                # get stage_height
                stage_height = 0
                for node_value in stage.values(): stage_height += node_value
                # get x axis
                nodes_order = list(dict(sorted(self.settings['node_order'].items(), key=lambda item: item[1])).keys())
                # for first node
                first_prinode = nodes_order[0]
                _node_index = [_node_index for _node_index in stage if len(re.findall(first_prinode, display_labels[_node_index])) > 0][0]
                sankey_node_order['y_axises'][_node_index] = 0.001
                cur_node_value = stage[_node_index]
                last_node_values.append((0.001, cur_node_value))
                
                # for second node
                second_prinode = nodes_order[1]
                _node_index = [_node_index for _node_index in stage if len(re.findall(second_prinode, display_labels[_node_index])) > 0][0]
                last_node_value = last_node_values.pop()
                cur_node_xaxis = last_node_value[0] + (last_node_value[1] / stage_height) - 0.15
                cur_node_xaxis = cur_node_xaxis if cur_node_xaxis > 0.001 else last_node_value[0] + (last_node_value[1] / stage_height)
                cur_node_value = stage[_node_index]
                sankey_node_order['y_axises'][_node_index] = cur_node_xaxis
                last_node_values.append((cur_node_xaxis, cur_node_value))

                # for other node
                for prinode in nodes_order[2:]:
                    try:
                        _node_index = [_node_index for _node_index in stage if len(re.findall(prinode, display_labels[_node_index])) > 0][0]
                    except Exception as e:
                        print(e)
                        continue
                    
                    last_node_value = last_node_values.pop()
                    
                    cur_node_xaxis = last_node_value[0] + (last_node_value[1] / stage_height)
                    cur_node_value = stage[_node_index]
                    
                    sankey_node_order['y_axises'][_node_index] = cur_node_xaxis
                    last_node_values.append((cur_node_xaxis, cur_node_value)) 

            self.sankey_input["sankey_node_order"] = sankey_node_order
    
    def _set_sankey_chart(self):
        sources = self.sankey_input['sources']
        targets = self.sankey_input['targets']
        values = self.sankey_input['values']
        links_percent = self.sankey_input['links_percent']
        sorted_display_labels = dict(sorted(self.sankey_input['display_labels'].items()))
        sorted_x_axis = list(dict(sorted(self.sankey_input['sankey_node_order']['x_axises'].items())).values()) if self.sankey_input['sankey_node_order'] is not None else None
        sorted_y_axis = list(dict(sorted(self.sankey_input['sankey_node_order']['y_axises'].items())).values()) if self.sankey_input['sankey_node_order'] is not None else None
        
        # coloring
        sorted_nodes_color = list(dict(sorted(self.sankey_input['nodes_color'].items())).values())
        bg_color = "white"
        
        
        
        # setting order
        if sorted_x_axis is None:
            node = dict(
                    pad = 15,
                    thickness = 30,
                    line = dict(color = "black", width = 0.5),
                    label = list(sorted_display_labels.values()),  # Customized label
                    hovertemplate = '%{value}<extra></extra>',
                    color = sorted_nodes_color,
            )
        else:
            node = dict(
                    pad = 15,
                    thickness = 30,
                    line = dict(color = "black", width = 0.5),
                    label = list(sorted_display_labels.values()),  # Customized label
                    hovertemplate = '%{value}<extra></extra>',
                    color = sorted_nodes_color,
                    x = sorted_x_axis,
                    y = sorted_y_axis
            )
        
        link = dict(
                source = sources,
                target = targets,
                value = values,
                customdata = links_percent,
                hovertemplate = '%{customdata}%<extra></extra>',
        )
        
        self.fig = go.Figure(go.Sankey(
            arrangement='snap',
            node = node,
            link = link
        ))
            
        self.fig.update_layout(
            title={
                'text': f"Sankey Chart created at {datetime.now().strftime('%H-%M-%S')}",
                'y':0.95,
                'x':0.5,
                'xanchor':'center',
                'yanchor':'top',
                'font_size':30
            },
            font=dict(size=12, color="#3C3D37", family="Arial Black"),  # General font for labels
            plot_bgcolor="lightyellow",  # Plot background color
            paper_bgcolor=bg_color,   # Paper background color
            hoverlabel=dict(font_size=16, font_family="Helvetica"),  # Hover label font customization
            annotations = self.annotations
        )
    
    def _export_sankey(self):
        self.fig.write_html(os.path.join(os.getcwd(), Settings.OUT_HTML_NAME))
    
    
    # [START_HERE] public controler functions
    def prepare_sankey(self, input_data_path, stage_cols_map, val_col, val_agg, custom_settings = None):
        self.input_data_path = input_data_path
        self.stage_cols_map = stage_cols_map
        self.val_col = val_col
        self.val_agg = val_agg
        self.custom_settings = custom_settings
        
    def make_sankey(self):
        self._read_data() 
        self._rename_nodes()
        # prepare customisation settings
        self._prepare_custom_settings()
        # render sankey
        self._set_sankey_input()
        self._set_stage_names()
        self._set_sankey_node_order()
        self._set_sankey_chart()
        # export sankey
        self._export_sankey()
        
        
    