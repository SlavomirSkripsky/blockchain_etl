# blockchain_etl
Táto práca sa zameriava na analýzu blockchainových transakcií kryptomien Bitcoin, Ethereum a Litecoin. Hlavným cieľom je porozumieť:

rozdeleniu transakcií medzi jednotlivé blockchainové siete,

optimalizácii transakčných poplatkov (gas fees),

vlastnostiam adries a smart kontraktov,

časovému vývoju a dynamike transakčnej aktivity.

Vstupné dáta pochádzajú z databázy Amberdata Blockchain, ktorá je dostupná prostredníctvom Snowflake Marketplace v schéme BLOCKCHAIN_CRYPTO_DATA.AMBERDATA_BLOCKCHAIN. Dataset obsahuje štruktúrované informácie o pending transakciách pre tri kryptomeny, ktoré boli zjednotené do staging tabuľky UNIFIED_STG.

Úlohou ELT pipeline bolo integrovať heterogénne blockchainové dáta, vykonať ich čistenie a transformáciu do hviezdicovej schémy, ktorá umožňuje efektívnu multidimenzionálnu analýzu transakčného správania.

Biznisové využitie a prínos analýzy

Výsledný dataset slúži ako základ pre blockchain analytiku a monitoring kryptomien, najmä v oblastiach:

tréningu modelov strojového učenia na predikciu transakčných poplatkov,

hodnotenia rizikovosti blockchainových adries (white/blacklist),

plánovania transakcií s ohľadom na čas a konkrétnu sieť,

porovnávania výkonnosti jednotlivých blockchain protokolov.

Dimenzionálny model vo forme hviezdicovej schémy umožňuje identifikovať kľúčové vzory, ako sú najviac zaťažené časové intervaly, efektívnosť jednotlivých sietí či výskyt transakčných anomálií. Tieto poznatky majú praktický význam pre kryptomenové burzy, peňaženky aj DeFi platformy.

1.1 Dátová architektúra
ERD diagram

Základné blockchainové dáta sú uložené v staging tabuľkách načítaných zo Snowflake Marketplace. Tieto tabuľky reprezentujú relačný model pending transakcií pre Bitcoin, Ethereum a Litecoin. Konceptuálna štruktúra zdrojových dát je znázornená pomocou entitno-relačného diagramu (ERD).

<p align="center">
  <img src="https://github.com/SlavomirSkripsky/blockchain_etl/blob/main/img/obr3.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma</em>
</p>

2. Dimenzionálny model

Implementovaný bol hviezdicový model (star schema) v súlade s Kimballovou metodológiou. Jadrom modelu je faktová tabuľka FACT_TRANSACTIONS, ktorá je prepojená so siedmimi dimenziami:

DIM_COIN – kryptomena a blockchain ID (identifikácia siete),

DIM_BLOCK – hash bloku a číslo bloku (blokový kontext),

DIM_TRANSACTION_TYPE – typ a stav transakcie, coinbase flag,

DIM_ADDRESS – odosielateľ, príjemca a kontraktové adresy,

DIM_TIME – časové atribúty (timestamp, dátum, hodina),

DIM_GAS – spotreba a cena gasu (ekonomická efektívnosť),

DIM_OP_PROTOCOL – protokolové a technické parametre.

Vizualizácia hviezdicovej schémy zobrazuje väzby medzi faktovou tabuľkou a dimenziami pomocou surrogátnych kľúčov generovaných hash funkciami, čo výrazne zvyšuje výkon analytických dotazov.

<p align="center">
  <img src="https://github.com/SlavomirSkripsky/blockchain_etl/blob/main/img/obr2.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy</em>
</p>

3. ELT proces v prostredí Snowflake

ELT proces pozostáva z troch základných krokov: Extract, Load a Transform, pričom jeho cieľom je previesť staging dáta do analytického multidimenzionálneho modelu.

3.1 Extrakcia (Extract)

Dáta zo Snowflake Marketplace boli skopírované do staging vrstvy vytvorením lokálnych tabuliek priamo zo zdrojových databáz. Tento krok pripravil údaje na ďalšie spracovanie.

#### Príklad kódu:
´´´sql
CREATE OR REPLACE TABLE STG_BTC_PENDING AS
SELECT * FROM BLOCKCHAIN_CRYPTO_DATA.AMBERDATA_BLOCKCHAIN.BITCOIN_PENDING_TRANSACTION;
´´´

3.2 Načítanie (Load)

Samostatné tabuľky pre jednotlivé kryptomeny boli zlúčené do spoločnej tabuľky UNIFIED_STG pomocou operácie UNION ALL. Každému záznamu bol pridaný atribút coin_name, ktorý určuje príslušnosť ku konkrétnej kryptomene.

Správnosť zlúčenia bola overená pomocou príkazov SELECT a DESCRIBE TABLE.

#### Príklad kódu:
´´´sql
CREATE OR REPLACE TABLE UNIFIED_STG AS
SELECT *, 'BITCOIN' AS coin_name
FROM STG_BTC_PENDING
UNION ALL
SELECT *,'ETHEREUM' AS coin_name
FROM STG_ETH_PENDING
UNION ALL
SELECT *,'LITECOIN' AS coin_name
FROM STG_LTC_PENDING;
SELECT * FROM UNIFIED_STG;
´´´

3.3 Transformácia (Transform)

V tejto fáze prebehlo čistenie dát a ich transformácia do dimenzionálneho modelu. Pre každú dimenziu boli vytvorené surrogátne kľúče pomocou hash funkcií, čo zabezpečilo konzistentné prepojenia medzi tabuľkami.

Dimenzia DIM_COIN overuje identifikátory blockchainov a mapuje ich na konkrétne kryptomeny. Ostatné dimenzie (DIM_BLOCK, DIM_TRANSACTION_TYPE, DIM_ADDRESS, DIM_TIME, DIM_GAS, DIM_OP_PROTOCOL) rozširujú analytický kontext dát.

Faktová tabuľka FACT_TRANSACTIONS obsahuje numerické metriky, ako sú hodnota transakcie, poplatok či nonce, a využíva okenné (window) funkcie na výpočet poradia transakcií v bloku a kumulatívnych poplatkov.

Po úspešnom vytvorení analytického modelu boli staging tabuľky odstránené.

4. Vizualizácia dát

Dashboard zahŕňa päť hlavných vizualizácií transakčnej aktivity:

Počet transakcií podľa blockchainu – porovnanie aktivity medzi kryptomenami.

Transakcie v rámci bloku – počet transakcií, celkové a priemerné poplatky na blok.

Rozdelenie transakčných hodnôt – percentuálne zastúpenie hodnôt transakcií v intervaloch.

Vzťah medzi hodnotou a poplatkom – porovnanie fee a value podľa kryptomeny a stavu transakcie.

Transakcie s kontraktom vs. bez kontraktu – priemerná hodnota transakcií podľa typu adresy.

Obrázok 5: Analytický dashboard blockchainových dát

<p align="center">
  <img src="https://github.com/SlavomirSkripsky/blockchain_etl/blob/main/img/obr1.png" alt="Dashboard">
  <br>
  <em>Obrázok 5 Dashboard Blockchain Crypto datasetu</em>
</p>

autor: Slavomír Skřipský
