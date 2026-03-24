# -*- coding: utf-8 -*-

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

from utilities import utils

# 從 Configuration 取得執行日期
target_date = spark.conf.get("today", None)
if target_date:
    today_dt = utils.parse_utc_day_to_tz_datetime(target_date, "Asia/Taipei")
    current = today_dt.date()
else:
    current = datetime.now(ZoneInfo("Asia/Taipei")).date()

# 設定計算天數
n_days = int(spark.conf.get("n_days", "31"))
start_date = current - timedelta(days=n_days)
end_date = current - timedelta(days=1)

# 資料來源
catalog = "prod"
source_schema = "current"
source_invoice = f"{catalog}.{source_schema}.invoice"
soruce_bu = f"{catalog}.{source_schema}.bu"
source_pos = f"{catalog}.{source_schema}.pos_master_member"

# 設定 target(建立 table 的位置)
target_raw = f"{catalog}.rfm.raw"
target_rfm = f"{catalog}.rfm.rfm"

@dp.materialized_view(
  name=target_raw,
  cluster_by_auto = True,
)
def prepare_rfm():
    df = spark.read.table(source_invoice)
    df = df.filter(
        (F.col("invoice_date") >= start_date) &
        (F.col("invoice_date") <= end_date) &
        (F.col("amount") > 0) &
        (F.length("gid") == 17) 
    )

    w = Window.partitionBy("invoice_number").orderBy("cre_date", "cre_time")
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

    bu = spark.read.table(soruce_bu).filter(
        (F.col("invoice_date") >= start_date) &
        (F.col("invoice_date") <= end_date) &
        (F.col("seq_number") == "001")
    ).select("invoice_number", F.lit("集團內").alias("group_flag"))
    df = df.join(bu, on="invoice_number", how="left")
    df = df.withColumn("group_flag", F.coalesce(F.col("group_flag"), F.lit("集團外")))

    """
    7-ELEVEN(統一超商) vs 全家(全家便利商店), 萊爾富(萊爾富國際), OK(來來超商), 美廉社(三商家購)
    康是美(統一生活事業, 康是美) vs 屈臣氏(台灣屈臣氏個人用品), 寶雅(寶雅國際)
    星巴克(星巴克 悠旅) vs 路易莎(路易莎職人咖啡 路易莎), 85度C(美食達人股份), Dreamers Coffee(好膳企業 金星生活), cama(咖碼股份)
    百貨: 統一時代(統一時代台北, 夢時代+統一時代) 
         vs 新光三越(新光三越), 遠東百貨(遠東百貨), SOGO(SOGO 太平洋崇光), 微風(微風置地 微風股份 微風廣場實業)
    百貨北: ^統一時代台北|^統一百華股份有限公司台北分公司$
        vs 微風置地股份有限公司|^微風股份有限公司|微風廣場實業股份有限公司
            (開在百貨的廠商) 微風廣場店|微風松高|微風南山|微風信義|微風南京
            (政府BOT) 微風東岸|微風台北車站|微風台大醫院|微風廣場實業股份有限公司台大分公司|微風廣場實業股份有限公司中央研究院分公司
           遠東百貨股份有限公司信義|遠東百貨股份有限公司板橋|遠東百貨股份有限公司$
            (其他百貨分店) 遠東百貨股份有限公司新竹|遠東百貨股份有限公司台中|遠東百貨股份有限公司花蓮|遠東百貨股份有限公司桃園|遠東百貨股份有限公司竹北
            (量販超市) 愛買(遠百企業股份有限公司) 開在百貨內
            (開在百貨的廠商) 遠百信義營業所|竹北遠百營業所|新竹大遠百營業所|台中遠百店|台中大遠百門市|台中大遠百店|台中遠百營業所|花蓮遠百店
           太平洋崇光百貨股份有限公司遠東大巨蛋|太平洋崇光百貨股份有限公司敦化|太平洋崇光百貨股份有限公司復興|太平洋崇光百貨股份有限公司天母|太平洋崇光百貨股份有限公司$
            (其他百貨分店) 太平洋崇光百貨股份有限公司中壢|太平洋崇光百貨股份有限公司新竹
            (開在百貨的廠商)新加坡商海底撈事業股份有限公司台灣第一分公司台中廣三SOGO
           新光三越百貨股份有限公司台北|新光三越百貨股份有限公司$
            (其他百貨分店) 新光三越百貨股份有限公司桃園|新光三越百貨股份有限公司台中
            (開在百貨的廠商) 新光三越台中
    百貨南: ^夢時代\+統一時代|^統正開發股份有限公司$|^統一百華股份有限公司夢廣場分公司$
        vs 漢神購物中心股份有限公司高雄|漢神名店百貨股份有限公司$
            (開在百貨的廠商) 漢神巨蛋|高雄漢神       
           義大開發股份有限公司
            (開在百貨的廠商)高雄義大分公司|高雄義大營業所
           遠東百貨股份有限公司高雄
            (其他百貨分店) 遠東百貨股份有限公司台南|遠東百貨股份有限公司嘉義
            (開在百貨的廠商) 高雄大遠百|高雄遠百店|台南大遠百公園店|高雄遠百營業所|台南遠百營業所
           太平洋崇光百貨股份有限公司高雄
           ^新光三越百貨股份有限公司高雄三多|^新光三越百貨股份有限公司高雄左營
            (其他百貨分店) 新光三越百貨股份有限公司台南|新光三越百貨股份有限公司嘉義
            (開在百貨的廠商) 新光三越台南
    """
    df = df.select(
        "gid", "invoice_date", F.datediff(F.lit(current), F.col("invoice_date")).alias("Recency"),
        "invoice_number", "amount",
        "place_name", "group_flag",
        F.when(
            F.col("place_name").rlike(r"統一超商"), F.lit("7-ELEVEN")
        ).when(
            F.col("place_name").rlike(r"全家便利商店"), F.lit("Family-Mart")
        ).when(
            F.col("place_name").rlike(r"萊爾富國際"), F.lit("Hi-Life")
        ).when(
            F.col("place_name").rlike(r"來來超商"), F.lit("OK-Mart")
        ).when(
            F.col("place_name").rlike(r"三商家購"), F.lit("美廉社")
        ).when(
            F.col("place_name").rlike(r"統一生活事業|康是美"), F.lit("康是美")       
        ).when(
            F.col("place_name").rlike(r"台灣屈臣氏個人用品"), F.lit("屈臣氏")
        ).when(
            F.col("place_name").rlike(r"寶雅國際"), F.lit("寶雅")
        ).when(
            F.col("place_name").rlike(r"星巴克|悠旅"), F.lit("星巴克")
        ).when(
            F.col("place_name").rlike(r"路易莎職人咖啡|路易莎"), F.lit("路易莎")
        ).when(
            F.col("place_name").rlike(r"美食達人股份"), F.lit("85度C")
        ).when(
            F.col("place_name").rlike(r"好膳企業|金星生活"), F.lit("Dreamers Coffee")
        ).when(
            F.col("place_name").rlike(r"咖碼股份"), F.lit("Cama")
        ).when(
            F.col("place_name").rlike(r"^統一時代台北|^統一百華股份有限公司台北分公司$"), F.lit("統一時代台北")
        ).when(
            F.col("place_name").rlike(r"^夢時代\+統一時代|^統正開發股份有限公司$|^統一百華股份有限公司夢廣場分公司$"), F.lit("夢時代")
        ).when(
            F.col("place_name").rlike(r"^新光三越百貨股份有限公司(台北.+)?$"), F.lit("新光三越台北")
        ).when(
            F.col("place_name").rlike(r"^新光三越百貨股份有限公司高雄三多"), F.lit("新光三越三多")
        ).when(
            F.col("place_name").rlike(r"^新光三越百貨股份有限公司高雄左營"), F.lit("新光三越左營")
        ).when(
            F.col("place_name").rlike(r"^遠東百貨股份有限公司((信義|板橋.*)分公司)?$"), F.lit("遠東百貨雙北")
        ).when(
            F.col("place_name").rlike(r"^遠東百貨股份有限公司高雄分公司$"), F.lit("遠東百貨高雄")
        ).when(
            F.col("place_name").rlike(r"^太平洋崇光百貨股份有限公司((遠東大巨蛋|敦化|復興|天母)分公司)?$"), F.lit("SOGO台北")
        ).when(
            F.col("place_name").rlike(r"^太平洋崇光百貨股份有限公司高雄分公司$"), F.lit("SOGO高雄")
        ).when(
            F.col("place_name").rlike(r"^微風置地股份有限公司|^微風股份有限公司$|^微風廣場實業股份有限公司$"), F.lit("微風廣場")
        ).when(
            F.col("place_name").rlike(r"^漢神購物中心股份有限公司高雄分公司$"), F.lit("漢神巨蛋")
        ).when(
            F.col("place_name").rlike(r"^漢神名店百貨股份有限公司$"), F.lit("漢神百貨")
        ).when(
            F.col("place_name").rlike(r"^義大開發股份有限公司$"), F.lit("義大世界")
        ).otherwise(F.lit("其他")
        ).alias("company")
    )
    df = df.withColumn("group_flag",
        F.when(
            F.col("company").isin("7-ELEVEN", "康是美", "星巴克", "統一時代台北", "夢時代"), F.lit("集團內")
        ).otherwise(F.col("group_flag")
        )
    )
    df = df.withColumn("company_flag",
        F.when(
            F.col("company").isin("7-ELEVEN", "Family-Mart", "Hi-Life", "OK-Mart", "美廉社"), F.lit("超商")
        ).when(
            F.col("company").isin("康是美", "屈臣氏", "寶雅",), F.lit("藥妝")
        ).when(
            F.col("company").isin("星巴克", "路易莎", "85度C", "Dreamers Coffee", "Cama"), F.lit("咖啡廳")
        ).when(
            F.col("company").isin("統一時代台北", "新光三越台北", "遠東百貨雙北", "SOGO台北", "微風廣場"), F.lit("百貨北")
        ).when(
            F.col("company").isin("夢時代", "新光三越三多", "新光三越左營", "遠東百貨高雄", "SOGO高雄", "漢神巨蛋", "漢神百貨", "義大世界"), F.lit("百貨南")
        ).otherwise(
            F.lit("其他")
        )
    )
    df = df.withColumn("company",
        F.when(
            (F.col("group_flag") == "集團內") & (F.col("company") == "其他"), F.lit("其他-BU")
        ).when(
            (F.col("group_flag") == "集團外") & (F.col("company") == "其他"), F.lit("其他-集團外")
        ).otherwise(F.col("company"))
    )

    pos = spark.read.table(source_pos).filter(
        (F.col("data_date") >= start_date) &
        (F.col("data_date") <= end_date) &
        (F.col("total_sm_of_mny") > 0) &
        (F.length("mid") == 32) &
        (F.col("job_id") == '23')
    ).select(
        F.col("mid").alias("gid"), F.col("data_date").alias("invoice_date"),
        F.datediff(F.lit(current), F.col("data_date")).alias("Recency"),
        F.col("rec_no").alias("invoice_number"),
        F.col("total_sm_of_mny").alias("amount"),
        F.col("store_name").alias("place_name"), 
        F.lit("集團內").alias("group_flag"),
        F.lit("7-ELEVEN").alias("company"),
        F.lit("超商").alias("company_flag")
    )
    return df.unionByName(pos)

@dp.materialized_view(
  name=target_rfm,
  cluster_by_auto = True,
)
def rfm():
    df = spark.read.table(target_raw)
    df = df.groupBy("gid", "group_flag", "company_flag", "company").agg(
        F.min("recency").alias("recency"),
        F.count("invoice_number").alias("frequency"),
        F.sum("amount").alias("monetary")
    )
    return df