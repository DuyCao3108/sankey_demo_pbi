from SankeyMakerClass import SankeyMaker
import os


# MANDATORY
INPUT_DATA_PATH = os.path.join(os.getcwd(), "input_data.csv")
STAGE_COLS_MAP = {
    "INI_CONTRACT_TYPE": "INIT",
    "CONTRACT1_TYPE": "C1",
    "CONTRACT2_TYPE": "C2",
    "CONTRACT3_TYPE": "C3"
}
VALUE_COLUMN = "CLIENTS"
AGG_FOR_VALUE_COLUMN = "sum" 

# OPTIONAL
CUSTOM_SETTINGS = {
    "node_order": {
        "CD":0,
        "TW":1,
        "CL":2,
        "CC":3,
        "HPL":4,
        "NC":5
    },
    "color_theme_name": "THEME_1", # watch out CASE
    "color_overwrite": {
        "NC": "#3C3D37"
    },
    "node_tohide_sr_tar": "NC"
}



if __name__ == "__main__":
    sankey = SankeyMaker()
    
    # prepare input
    sankey.prepare_sankey(
        input_data_path=INPUT_DATA_PATH,
        stage_cols_map=STAGE_COLS_MAP,
        val_col=VALUE_COLUMN,
        val_agg=AGG_FOR_VALUE_COLUMN,
        custom_settings=CUSTOM_SETTINGS
    )
    
    sankey.make_sankey()