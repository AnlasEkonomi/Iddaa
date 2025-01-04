import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

scraper=cloudscraper.create_scraper()

def iddaa_bilgi(basla,son):
    url="https://www.mackolik.com/perform/p0/ajax/components/competition/livescores/json?"
    basla_tarih=datetime.strptime(basla,"%Y-%m-%d")
    son_tarih=datetime.strptime(son,"%Y-%m-%d")

    tum_veri=pd.DataFrame()

    while basla_tarih <= son_tarih:
        tarih_str=basla_tarih.strftime("%Y-%m-%d")
        params={"sports[]":"Soccer","matchDate":tarih_str}
        r=scraper.get(url,params=params).json()["data"]["matches"]

        id,mac,skor,iyskor,iddiakod,tarih_list=[], [], [], [], [], []

        for i in r:
            id.append(i)
            tarih_list.append(r[i]["mstUtc"])
            mac.append(r[i]["matchName"])
            score=f"{r[i]['score'].get('home')}-{r[i]['score'].get('away')}"
            skor.append(score)
            try:
                iy_skor=f"{r[i]['score']['ht'].get('home')}-{r[i]['score']['ht'].get('away')}"
                iyskor.append(iy_skor)
            except AttributeError:
                iyskor.append("Veri Yok")
            iddiakod.append(str(r[i].get("iddaaCode")))

        veri=pd.DataFrame({"Tarih":tarih_list,"Maç":mac,"İY Skor":iyskor,"Skor":skor,
                             "İddia Kodu":iddiakod,"ID":id})

        veri=veri[veri["İddia Kodu"] != "None"]
        veri.drop("İddia Kodu",axis=1,inplace=True)

        veri["Tarih"]=pd.to_datetime(veri["Tarih"],unit="ms")
        veri["Tarih"]=veri["Tarih"].dt.strftime("%d-%m-%Y")
        veri.reset_index(drop=True,inplace=True)

        tum_veri=pd.concat([tum_veri,veri],ignore_index=True)
        basla_tarih += timedelta(days=1)
    return tum_veri


def get_bahis_oranlari(i,tarih,mac,skor,iyskor):
    url=f"https://www.mackolik.com/mac/gaziantep-fk-vs-pendikspor/iddaa/{i}"
    r=scraper.get(url).text
    s=BeautifulSoup(r,"html.parser")
    ul=s.find("ul",{"class":"widget-iddaa-markets__markets-list"})

    try:
        h2_tags=ul.find_all("h2")
        bahistipi=[h2.find("span").text for h2 in h2_tags]

        div_tags=ul.find_all("div", {"class": "widget-base__content widget-iddaa-markets__market-content"})
        ul_tags=[div.find("ul") for div in div_tags]

        bahisoranlar=[]
        for ul in ul_tags:
            li_texts=[]
            for li in ul.find_all("li"):
                span_texts=[span.get_text(strip=True) for span in li.find_all("span")]
                li_texts.append(span_texts)
            bahisoranlar.append(li_texts)

        bahis_sozluk={}
        for a in range(len(bahistipi)):
            for j in range(len(bahisoranlar[a])):
                for k in range(0,len(bahisoranlar[a][j]),2):
                    key=f"{bahistipi[a]} ({bahisoranlar[a][j][k]})"
                    value=bahisoranlar[a][j][k+1]
                    bahis_sozluk[key]=value

        bahis_df=pd.DataFrame(list(bahis_sozluk.items()),columns=["Bahis Türü","Oran"])
        bahis_df["Tarih"]=tarih
        bahis_df["Maç"]=mac
        bahis_df["İY Skor"]=iyskor
        bahis_df["Skor"]=skor
        bahis_df["Oran"]=bahis_df["Oran"].apply(lambda x: str(x).replace('.', ','))

        return bahis_df

    except AttributeError:
        return None

def iddaa_bahis_oranlari(basla, son):
    ids=iddaa_bilgi(basla,son)["ID"]
    mac=iddaa_bilgi(basla,son)["Maç"]
    iyskor=iddaa_bilgi(basla,son)["İY Skor"]
    skor=iddaa_bilgi(basla,son)["Skor"]
    tarih=iddaa_bilgi(basla,son)["Tarih"]

    tum_bahis_df=pd.DataFrame()

    with ThreadPoolExecutor() as executor:
        futures=[executor.submit(get_bahis_oranlari,i,tarih.iloc[idx],mac.iloc[idx],iyskor.iloc[idx],skor.iloc[idx]) for idx, i in enumerate(ids)]
        for future in futures:
            bahis_df=future.result()
            if bahis_df is not None:
                tum_bahis_df=pd.concat([tum_bahis_df, bahis_df],ignore_index=True)

    tum_bahis_df=tum_bahis_df[["Tarih","Maç","İY Skor","Skor","Bahis Türü","Oran"]]
    tum_bahis_df.to_excel("bahis_oranlari.xlsx",index=False)


# Tarihleri yıl-ay-gün şeklinde giriniz
iddaa_bahis_oranlari("2024-12-31","2024-12-31")