import pandas as pd
import numpy as np
from create_accident_table import (COL_MAP,
                    MAIN_COL as MC,
                    ENVIRONMENT_COL as EC,
                    HUMAN_BEAHAVIOR_COL as HBC,
                    EVENT_PROCESS_PARTICIPATE_OBJECT_COL as EPPOC,
                    EVENT_RESULT_COL as ERC)



# ------------------------------------------------------------------
# 建立對應字典,歷年車禍資料
# ------------------------------------------------------------------
def transform_data_dict(df_year_list)->list:    
    rename_data = []
    
    for df in df_year_list:
        df = df.rename(columns = COL_MAP)
        rename_data.append(df)
    
    return rename_data

# ------------------------------------------------------------------
# 歷年資料清洗
# ------------------------------------------------------------------
def car_crash_old_data_clean(df_year_list):
    primary_data_list = []
    all_parties_list = []
    cnt=0
    # def create_hash(row):
    #     input_data = (
    #         f"{row['accident_year']}|{row['accident_month']}|{row['accident_date']}|"
    #         f"{row['accident_time']}|{row['speed_limit_primary_party']}|{row['accident_type_minor']}|"
    #         f"{row['casualties_count']}|{row['longitude']}|{row['latitude']}"
    #         )
    #     return hashlib.sha256(input_data.encode('utf-8')).hexdigest()[:16]
    
    for df in df_year_list:
        cnt+=1
        # if 'accident_id' not in df.columns:
        #  # Pandas 會自動把每一列塞進 create_hash 的第一個位置
        #     df.insert(0, 'accident_id', df.apply(create_hash, axis=1))
        #     #df.insert(loc, column, value)：
        if 'accident_id' not in df.columns:
            sort_cols = ['accident_year', 'accident_month', 'accident_date', 'accident_time', 'longitude', 'latitude', 'party_sequence']
            df = df.sort_values(by=sort_cols).reset_index(drop=True)
        # 1. 生成當前檔案的局部流水號 (1, 2, 3...)
            local_id = (df['party_sequence'] == 1).cumsum()
        # 2. 取得年份 (確保是整數，避免出現 2021.0)
            year_val = int(float(df['accident_year'].iloc[0]))
            month_val = int(float(df['accident_month'].iloc[0])) 
        # 3. 組合：年份 + 6位補零流水號 (例如: 2021000001)
            df['accident_id'] = local_id.apply(lambda x: f"{year_val}{month_val:02d}{cnt:02d}{x:06d}")

        invalid_gender = ~df["gender"].isin(["男", "女"])
        df.loc[invalid_gender, ["gender", "age"]] = np.nan
        df = df[~df['accident_year'].astype(str).str.contains('資料提供日期|事故類別', na=False)]
        if 'casualties_count' in df.columns:    
        # 直接找「數字」後面跟著「死」或「傷」的組合
            df['death_count'] = df['casualties_count'].str.extract(r'死亡(\d+)').fillna(0).infer_objects(copy=False).astype(int)
            df['injury_count'] = df['casualties_count'].str.extract(r'受傷(\d+)').fillna(0).infer_objects(copy=False).astype(int)
            df = df.drop(columns=['casualties_count'])

        if 'lane_edge_marking' in df.columns:
            df['lane_edge_marking'] = df['lane_edge_marking'].map({'有': True, '無': False}).fillna(False).infer_objects(copy=False).astype(bool)
        
        
        if 'accident_month' in df.columns:
            df['accident_month'] = df['accident_month'].astype(str).str.replace(r'\.0', '', regex=True)
            #原始字串 r'\.'：Python 看到 r 就會「兩手一攤」，把 \. 原封不動交給 Pandas 的 Regex 引擎。Regex 引擎認得 \. 是小數點
        if 'accident_date' in df.columns:                                       
            df['accident_date'] = df['accident_date'].astype(str).str.replace(r'\.0', '', regex=True).str[-2:]
        if 'accident_time' in df.columns:
            df['accident_time'] = df['accident_time'].astype(str).str.replace(r'\.0', '', regex=True).str.zfill(6)
            #zfill(6)從左側補滿6瑪'0', ex:'142'=>'000142'                       #'\.0', '' ==>把字串中的".0"替換成空字串 
            
        register=df['accident_time']#暫存在register省效能
        hour = register.str[0:2]
        minute = register.str[2:4]
        second = register.str[4:6]
        
        if 'accident_datetime' not in df.columns:  
            temp_dt = pd.to_datetime({
            #pd.to_datetime不用寫insert也會自己插入表格
                'year': df['accident_year'],
                'month': df['accident_month'],
                'day': df['accident_date'],
                'hour': hour,
                'minute': minute,
                'second': second
                }, errors='coerce')
            df.insert(2, 'accident_datetime',temp_dt)
            df.insert(3, 'accident_weekday',temp_dt.dt.dayofweek)
            cols_to_remove = ['accident_time', 'accident_year', 'accident_month', 'accident_date']
            df = df.drop(columns=cols_to_remove)
            
            
        if 'weather_condition' in df.columns:
            df['weather_condition'] = df['weather_condition'].astype(str).str.slice(0, 10)
        
        if 'party_sequence' in df.columns:
            df_primary = df[df['party_sequence']==1].copy()
            #把只含df['party_sequence']==1 的條件存到df_primary這個新的dataframe
            primary_data_list.append(df_primary)
            df_other = df[df['party_sequence']!=1].copy()
            #df_other = df_other[~df_other['accident_year'].astype(str).str.contains('資料提供日期|事故類別', na=False)]
            all_parties_list.append(df_other)
            
            
        
        if (not primary_data_list) and (not all_parties_list):
            return pd.DataFrame(), pd.DataFrame()
        

    #final_main_df = pd.concat(primary_data_list, ignore_index=True).drop_duplicates(subset=['accident_id'], keep='first')
    final_main_df = pd.concat(primary_data_list, ignore_index=True).drop_duplicates()
    #final_all_party_df=pd.concat(all_parties_list, ignore_index=True).drop_duplicates(subset=['accident_id','party_sequence'], keep='first')
    final_all_party_df=pd.concat(all_parties_list, ignore_index=True).drop_duplicates()

    return {
    "main": {
        "master": final_main_df[MC],
        "env": final_main_df[EC],
        "human": final_main_df[HBC],
        "process": final_main_df[EPPOC],
        "result": final_main_df[ERC]
    },
    "party": {
        "master": final_all_party_df[MC],
        "env": final_all_party_df[EC],
        "human": final_all_party_df[HBC],
        "process": final_all_party_df[EPPOC],
        "result": final_all_party_df[ERC]
    }
}
        


            
