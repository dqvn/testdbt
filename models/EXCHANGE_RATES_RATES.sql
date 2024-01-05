{{ config (
materialized="table"
)}}
            
with __dbt__cte__EXCHANGE_RATES_RATES_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "WTTDEMO".PUBLIC."EXCHANGE_RATES"
select
    _AIRBYTE_EXCHANGE_RATES_HASHID,
    to_varchar(get_path(parse_json(RATES), '"AED"')) as AED,
    to_varchar(get_path(parse_json(RATES), '"AFN"')) as AFN,
    to_varchar(get_path(parse_json(RATES), '"ALL"')) as "ALL",
    to_varchar(get_path(parse_json(RATES), '"AMD"')) as AMD,
    to_varchar(get_path(parse_json(RATES), '"ANG"')) as ANG,
    to_varchar(get_path(parse_json(RATES), '"AOA"')) as AOA,
    to_varchar(get_path(parse_json(RATES), '"ARS"')) as ARS,
    to_varchar(get_path(parse_json(RATES), '"AUD"')) as AUD,
    to_varchar(get_path(parse_json(RATES), '"AWG"')) as AWG,
    to_varchar(get_path(parse_json(RATES), '"AZN"')) as AZN,
    to_varchar(get_path(parse_json(RATES), '"BAM"')) as BAM,
    to_varchar(get_path(parse_json(RATES), '"BBD"')) as BBD,
    to_varchar(get_path(parse_json(RATES), '"BDT"')) as BDT,
    to_varchar(get_path(parse_json(RATES), '"BGN"')) as BGN,
    to_varchar(get_path(parse_json(RATES), '"BHD"')) as BHD,
    to_varchar(get_path(parse_json(RATES), '"BIF"')) as BIF,
    to_varchar(get_path(parse_json(RATES), '"BMD"')) as BMD,
    to_varchar(get_path(parse_json(RATES), '"BND"')) as BND,
    to_varchar(get_path(parse_json(RATES), '"BOB"')) as BOB,
    to_varchar(get_path(parse_json(RATES), '"BRL"')) as BRL,
    to_varchar(get_path(parse_json(RATES), '"BSD"')) as BSD,
    to_varchar(get_path(parse_json(RATES), '"BTC"')) as BTC,
    to_varchar(get_path(parse_json(RATES), '"BTN"')) as BTN,
    to_varchar(get_path(parse_json(RATES), '"BWP"')) as BWP,
    to_varchar(get_path(parse_json(RATES), '"BYN"')) as BYN,
    to_varchar(get_path(parse_json(RATES), '"BYR"')) as BYR,
    to_varchar(get_path(parse_json(RATES), '"BZD"')) as BZD,
    to_varchar(get_path(parse_json(RATES), '"CAD"')) as CAD,
    to_varchar(get_path(parse_json(RATES), '"CDF"')) as CDF,
    to_varchar(get_path(parse_json(RATES), '"CHF"')) as CHF,
    to_varchar(get_path(parse_json(RATES), '"CLF"')) as CLF,
    to_varchar(get_path(parse_json(RATES), '"CLP"')) as CLP,
    to_varchar(get_path(parse_json(RATES), '"CNY"')) as CNY,
    to_varchar(get_path(parse_json(RATES), '"COP"')) as COP,
    to_varchar(get_path(parse_json(RATES), '"CRC"')) as CRC,
    to_varchar(get_path(parse_json(RATES), '"CUC"')) as CUC,
    to_varchar(get_path(parse_json(RATES), '"CUP"')) as CUP,
    to_varchar(get_path(parse_json(RATES), '"CVE"')) as CVE,
    to_varchar(get_path(parse_json(RATES), '"CZK"')) as CZK,
    to_varchar(get_path(parse_json(RATES), '"DJF"')) as DJF,
    to_varchar(get_path(parse_json(RATES), '"DKK"')) as DKK,
    to_varchar(get_path(parse_json(RATES), '"DOP"')) as DOP,
    to_varchar(get_path(parse_json(RATES), '"DZD"')) as DZD,
    to_varchar(get_path(parse_json(RATES), '"EGP"')) as EGP,
    to_varchar(get_path(parse_json(RATES), '"ERN"')) as ERN,
    to_varchar(get_path(parse_json(RATES), '"ETB"')) as ETB,
    to_varchar(get_path(parse_json(RATES), '"EUR"')) as EUR,
    to_varchar(get_path(parse_json(RATES), '"FJD"')) as FJD,
    to_varchar(get_path(parse_json(RATES), '"FKP"')) as FKP,
    to_varchar(get_path(parse_json(RATES), '"GBP"')) as GBP,
    to_varchar(get_path(parse_json(RATES), '"GEL"')) as GEL,
    to_varchar(get_path(parse_json(RATES), '"GGP"')) as GGP,
    to_varchar(get_path(parse_json(RATES), '"GHS"')) as GHS,
    to_varchar(get_path(parse_json(RATES), '"GIP"')) as GIP,
    to_varchar(get_path(parse_json(RATES), '"GMD"')) as GMD,
    to_varchar(get_path(parse_json(RATES), '"GNF"')) as GNF,
    to_varchar(get_path(parse_json(RATES), '"GTQ"')) as GTQ,
    to_varchar(get_path(parse_json(RATES), '"GYD"')) as GYD,
    to_varchar(get_path(parse_json(RATES), '"HKD"')) as HKD,
    to_varchar(get_path(parse_json(RATES), '"HNL"')) as HNL,
    to_varchar(get_path(parse_json(RATES), '"HRK"')) as HRK,
    to_varchar(get_path(parse_json(RATES), '"HTG"')) as HTG,
    to_varchar(get_path(parse_json(RATES), '"HUF"')) as HUF,
    to_varchar(get_path(parse_json(RATES), '"IDR"')) as IDR,
    to_varchar(get_path(parse_json(RATES), '"ILS"')) as ILS,
    to_varchar(get_path(parse_json(RATES), '"IMP"')) as IMP,
    to_varchar(get_path(parse_json(RATES), '"INR"')) as INR,
    to_varchar(get_path(parse_json(RATES), '"IQD"')) as IQD,
    to_varchar(get_path(parse_json(RATES), '"IRR"')) as IRR,
    to_varchar(get_path(parse_json(RATES), '"ISK"')) as ISK,
    to_varchar(get_path(parse_json(RATES), '"JEP"')) as JEP,
    to_varchar(get_path(parse_json(RATES), '"JMD"')) as JMD,
    to_varchar(get_path(parse_json(RATES), '"JOD"')) as JOD,
    to_varchar(get_path(parse_json(RATES), '"JPY"')) as JPY,
    to_varchar(get_path(parse_json(RATES), '"KES"')) as KES,
    to_varchar(get_path(parse_json(RATES), '"KGS"')) as KGS,
    to_varchar(get_path(parse_json(RATES), '"KHR"')) as KHR,
    to_varchar(get_path(parse_json(RATES), '"KMF"')) as KMF,
    to_varchar(get_path(parse_json(RATES), '"KPW"')) as KPW,
    to_varchar(get_path(parse_json(RATES), '"KRW"')) as KRW,
    to_varchar(get_path(parse_json(RATES), '"KWD"')) as KWD,
    to_varchar(get_path(parse_json(RATES), '"KYD"')) as KYD,
    to_varchar(get_path(parse_json(RATES), '"KZT"')) as KZT,
    to_varchar(get_path(parse_json(RATES), '"LAK"')) as LAK,
    to_varchar(get_path(parse_json(RATES), '"LBP"')) as LBP,
    to_varchar(get_path(parse_json(RATES), '"LKR"')) as LKR,
    to_varchar(get_path(parse_json(RATES), '"LRD"')) as LRD,
    to_varchar(get_path(parse_json(RATES), '"LSL"')) as LSL,
    to_varchar(get_path(parse_json(RATES), '"LTL"')) as LTL,
    to_varchar(get_path(parse_json(RATES), '"LVL"')) as LVL,
    to_varchar(get_path(parse_json(RATES), '"LYD"')) as LYD,
    to_varchar(get_path(parse_json(RATES), '"MAD"')) as MAD,
    to_varchar(get_path(parse_json(RATES), '"MDL"')) as MDL,
    to_varchar(get_path(parse_json(RATES), '"MGA"')) as MGA,
    to_varchar(get_path(parse_json(RATES), '"MKD"')) as MKD,
    to_varchar(get_path(parse_json(RATES), '"MMK"')) as MMK,
    to_varchar(get_path(parse_json(RATES), '"MNT"')) as MNT,
    to_varchar(get_path(parse_json(RATES), '"MOP"')) as MOP,
    to_varchar(get_path(parse_json(RATES), '"MRO"')) as MRO,
    to_varchar(get_path(parse_json(RATES), '"MUR"')) as MUR,
    to_varchar(get_path(parse_json(RATES), '"MVR"')) as MVR,
    to_varchar(get_path(parse_json(RATES), '"MWK"')) as MWK,
    to_varchar(get_path(parse_json(RATES), '"MXN"')) as MXN,
    to_varchar(get_path(parse_json(RATES), '"MYR"')) as MYR,
    to_varchar(get_path(parse_json(RATES), '"MZN"')) as MZN,
    to_varchar(get_path(parse_json(RATES), '"NAD"')) as NAD,
    to_varchar(get_path(parse_json(RATES), '"NGN"')) as NGN,
    to_varchar(get_path(parse_json(RATES), '"NIO"')) as NIO,
    to_varchar(get_path(parse_json(RATES), '"NOK"')) as NOK,
    to_varchar(get_path(parse_json(RATES), '"NPR"')) as NPR,
    to_varchar(get_path(parse_json(RATES), '"NZD"')) as NZD,
    to_varchar(get_path(parse_json(RATES), '"OMR"')) as OMR,
    to_varchar(get_path(parse_json(RATES), '"PAB"')) as PAB,
    to_varchar(get_path(parse_json(RATES), '"PEN"')) as PEN,
    to_varchar(get_path(parse_json(RATES), '"PGK"')) as PGK,
    to_varchar(get_path(parse_json(RATES), '"PHP"')) as PHP,
    to_varchar(get_path(parse_json(RATES), '"PKR"')) as PKR,
    to_varchar(get_path(parse_json(RATES), '"PLN"')) as PLN,
    to_varchar(get_path(parse_json(RATES), '"PYG"')) as PYG,
    to_varchar(get_path(parse_json(RATES), '"QAR"')) as QAR,
    to_varchar(get_path(parse_json(RATES), '"RON"')) as RON,
    to_varchar(get_path(parse_json(RATES), '"RSD"')) as RSD,
    to_varchar(get_path(parse_json(RATES), '"RUB"')) as RUB,
    to_varchar(get_path(parse_json(RATES), '"RWF"')) as RWF,
    to_varchar(get_path(parse_json(RATES), '"SAR"')) as SAR,
    to_varchar(get_path(parse_json(RATES), '"SBD"')) as SBD,
    to_varchar(get_path(parse_json(RATES), '"SCR"')) as SCR,
    to_varchar(get_path(parse_json(RATES), '"SDG"')) as SDG,
    to_varchar(get_path(parse_json(RATES), '"SEK"')) as SEK,
    to_varchar(get_path(parse_json(RATES), '"SGD"')) as SGD,
    to_varchar(get_path(parse_json(RATES), '"SHP"')) as SHP,
    to_varchar(get_path(parse_json(RATES), '"SLL"')) as SLL,
    to_varchar(get_path(parse_json(RATES), '"SOS"')) as SOS,
    to_varchar(get_path(parse_json(RATES), '"SRD"')) as SRD,
    to_varchar(get_path(parse_json(RATES), '"STD"')) as STD,
    to_varchar(get_path(parse_json(RATES), '"SVC"')) as SVC,
    to_varchar(get_path(parse_json(RATES), '"SYP"')) as SYP,
    to_varchar(get_path(parse_json(RATES), '"SZL"')) as SZL,
    to_varchar(get_path(parse_json(RATES), '"THB"')) as THB,
    to_varchar(get_path(parse_json(RATES), '"TJS"')) as TJS,
    to_varchar(get_path(parse_json(RATES), '"TMT"')) as TMT,
    to_varchar(get_path(parse_json(RATES), '"TND"')) as TND,
    to_varchar(get_path(parse_json(RATES), '"TOP"')) as TOP,
    to_varchar(get_path(parse_json(RATES), '"TRY"')) as TRY,
    to_varchar(get_path(parse_json(RATES), '"TTD"')) as TTD,
    to_varchar(get_path(parse_json(RATES), '"TWD"')) as TWD,
    to_varchar(get_path(parse_json(RATES), '"TZS"')) as TZS,
    to_varchar(get_path(parse_json(RATES), '"UAH"')) as UAH,
    to_varchar(get_path(parse_json(RATES), '"UGX"')) as UGX,
    to_varchar(get_path(parse_json(RATES), '"USD"')) as USD,
    to_varchar(get_path(parse_json(RATES), '"UYU"')) as UYU,
    to_varchar(get_path(parse_json(RATES), '"UZS"')) as UZS,
    to_varchar(get_path(parse_json(RATES), '"VEF"')) as VEF,
    to_varchar(get_path(parse_json(RATES), '"VND"')) as VND,
    to_varchar(get_path(parse_json(RATES), '"VUV"')) as VUV,
    to_varchar(get_path(parse_json(RATES), '"WST"')) as WST,
    to_varchar(get_path(parse_json(RATES), '"XAF"')) as XAF,
    to_varchar(get_path(parse_json(RATES), '"XAG"')) as XAG,
    to_varchar(get_path(parse_json(RATES), '"XAU"')) as XAU,
    to_varchar(get_path(parse_json(RATES), '"XCD"')) as XCD,
    to_varchar(get_path(parse_json(RATES), '"XDR"')) as XDR,
    to_varchar(get_path(parse_json(RATES), '"XOF"')) as XOF,
    to_varchar(get_path(parse_json(RATES), '"XPF"')) as XPF,
    to_varchar(get_path(parse_json(RATES), '"YER"')) as YER,
    to_varchar(get_path(parse_json(RATES), '"ZAR"')) as ZAR,
    to_varchar(get_path(parse_json(RATES), '"ZMK"')) as ZMK,
    to_varchar(get_path(parse_json(RATES), '"ZMW"')) as ZMW,
    to_varchar(get_path(parse_json(RATES), '"ZWL"')) as ZWL,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "WTTDEMO".PUBLIC."EXCHANGE_RATES" as table_alias
-- RATES at exchange_rates/rates
where 1 = 1
and RATES is not null

),  __dbt__cte__EXCHANGE_RATES_RATES_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__EXCHANGE_RATES_RATES_AB1
select
    _AIRBYTE_EXCHANGE_RATES_HASHID,
    cast(AED as 
    float
) as AED,
    cast(AFN as 
    float
) as AFN,
    cast("ALL" as 
    float
) as "ALL",
    cast(AMD as 
    float
) as AMD,
    cast(ANG as 
    float
) as ANG,
    cast(AOA as 
    float
) as AOA,
    cast(ARS as 
    float
) as ARS,
    cast(AUD as 
    float
) as AUD,
    cast(AWG as 
    float
) as AWG,
    cast(AZN as 
    float
) as AZN,
    cast(BAM as 
    float
) as BAM,
    cast(BBD as 
    float
) as BBD,
    cast(BDT as 
    float
) as BDT,
    cast(BGN as 
    float
) as BGN,
    cast(BHD as 
    float
) as BHD,
    cast(BIF as 
    float
) as BIF,
    cast(BMD as 
    float
) as BMD,
    cast(BND as 
    float
) as BND,
    cast(BOB as 
    float
) as BOB,
    cast(BRL as 
    float
) as BRL,
    cast(BSD as 
    float
) as BSD,
    cast(BTC as 
    float
) as BTC,
    cast(BTN as 
    float
) as BTN,
    cast(BWP as 
    float
) as BWP,
    cast(BYN as 
    float
) as BYN,
    cast(BYR as 
    float
) as BYR,
    cast(BZD as 
    float
) as BZD,
    cast(CAD as 
    float
) as CAD,
    cast(CDF as 
    float
) as CDF,
    cast(CHF as 
    float
) as CHF,
    cast(CLF as 
    float
) as CLF,
    cast(CLP as 
    float
) as CLP,
    cast(CNY as 
    float
) as CNY,
    cast(COP as 
    float
) as COP,
    cast(CRC as 
    float
) as CRC,
    cast(CUC as 
    float
) as CUC,
    cast(CUP as 
    float
) as CUP,
    cast(CVE as 
    float
) as CVE,
    cast(CZK as 
    float
) as CZK,
    cast(DJF as 
    float
) as DJF,
    cast(DKK as 
    float
) as DKK,
    cast(DOP as 
    float
) as DOP,
    cast(DZD as 
    float
) as DZD,
    cast(EGP as 
    float
) as EGP,
    cast(ERN as 
    float
) as ERN,
    cast(ETB as 
    float
) as ETB,
    cast(EUR as 
    float
) as EUR,
    cast(FJD as 
    float
) as FJD,
    cast(FKP as 
    float
) as FKP,
    cast(GBP as 
    float
) as GBP,
    cast(GEL as 
    float
) as GEL,
    cast(GGP as 
    float
) as GGP,
    cast(GHS as 
    float
) as GHS,
    cast(GIP as 
    float
) as GIP,
    cast(GMD as 
    float
) as GMD,
    cast(GNF as 
    float
) as GNF,
    cast(GTQ as 
    float
) as GTQ,
    cast(GYD as 
    float
) as GYD,
    cast(HKD as 
    float
) as HKD,
    cast(HNL as 
    float
) as HNL,
    cast(HRK as 
    float
) as HRK,
    cast(HTG as 
    float
) as HTG,
    cast(HUF as 
    float
) as HUF,
    cast(IDR as 
    float
) as IDR,
    cast(ILS as 
    float
) as ILS,
    cast(IMP as 
    float
) as IMP,
    cast(INR as 
    float
) as INR,
    cast(IQD as 
    float
) as IQD,
    cast(IRR as 
    float
) as IRR,
    cast(ISK as 
    float
) as ISK,
    cast(JEP as 
    float
) as JEP,
    cast(JMD as 
    float
) as JMD,
    cast(JOD as 
    float
) as JOD,
    cast(JPY as 
    float
) as JPY,
    cast(KES as 
    float
) as KES,
    cast(KGS as 
    float
) as KGS,
    cast(KHR as 
    float
) as KHR,
    cast(KMF as 
    float
) as KMF,
    cast(KPW as 
    float
) as KPW,
    cast(KRW as 
    float
) as KRW,
    cast(KWD as 
    float
) as KWD,
    cast(KYD as 
    float
) as KYD,
    cast(KZT as 
    float
) as KZT,
    cast(LAK as 
    float
) as LAK,
    cast(LBP as 
    float
) as LBP,
    cast(LKR as 
    float
) as LKR,
    cast(LRD as 
    float
) as LRD,
    cast(LSL as 
    float
) as LSL,
    cast(LTL as 
    float
) as LTL,
    cast(LVL as 
    float
) as LVL,
    cast(LYD as 
    float
) as LYD,
    cast(MAD as 
    float
) as MAD,
    cast(MDL as 
    float
) as MDL,
    cast(MGA as 
    float
) as MGA,
    cast(MKD as 
    float
) as MKD,
    cast(MMK as 
    float
) as MMK,
    cast(MNT as 
    float
) as MNT,
    cast(MOP as 
    float
) as MOP,
    cast(MRO as 
    float
) as MRO,
    cast(MUR as 
    float
) as MUR,
    cast(MVR as 
    float
) as MVR,
    cast(MWK as 
    float
) as MWK,
    cast(MXN as 
    float
) as MXN,
    cast(MYR as 
    float
) as MYR,
    cast(MZN as 
    float
) as MZN,
    cast(NAD as 
    float
) as NAD,
    cast(NGN as 
    float
) as NGN,
    cast(NIO as 
    float
) as NIO,
    cast(NOK as 
    float
) as NOK,
    cast(NPR as 
    float
) as NPR,
    cast(NZD as 
    float
) as NZD,
    cast(OMR as 
    float
) as OMR,
    cast(PAB as 
    float
) as PAB,
    cast(PEN as 
    float
) as PEN,
    cast(PGK as 
    float
) as PGK,
    cast(PHP as 
    float
) as PHP,
    cast(PKR as 
    float
) as PKR,
    cast(PLN as 
    float
) as PLN,
    cast(PYG as 
    float
) as PYG,
    cast(QAR as 
    float
) as QAR,
    cast(RON as 
    float
) as RON,
    cast(RSD as 
    float
) as RSD,
    cast(RUB as 
    float
) as RUB,
    cast(RWF as 
    float
) as RWF,
    cast(SAR as 
    float
) as SAR,
    cast(SBD as 
    float
) as SBD,
    cast(SCR as 
    float
) as SCR,
    cast(SDG as 
    float
) as SDG,
    cast(SEK as 
    float
) as SEK,
    cast(SGD as 
    float
) as SGD,
    cast(SHP as 
    float
) as SHP,
    cast(SLL as 
    float
) as SLL,
    cast(SOS as 
    float
) as SOS,
    cast(SRD as 
    float
) as SRD,
    cast(STD as 
    float
) as STD,
    cast(SVC as 
    float
) as SVC,
    cast(SYP as 
    float
) as SYP,
    cast(SZL as 
    float
) as SZL,
    cast(THB as 
    float
) as THB,
    cast(TJS as 
    float
) as TJS,
    cast(TMT as 
    float
) as TMT,
    cast(TND as 
    float
) as TND,
    cast(TOP as 
    float
) as TOP,
    cast(TRY as 
    float
) as TRY,
    cast(TTD as 
    float
) as TTD,
    cast(TWD as 
    float
) as TWD,
    cast(TZS as 
    float
) as TZS,
    cast(UAH as 
    float
) as UAH,
    cast(UGX as 
    float
) as UGX,
    cast(USD as 
    float
) as USD,
    cast(UYU as 
    float
) as UYU,
    cast(UZS as 
    float
) as UZS,
    cast(VEF as 
    float
) as VEF,
    cast(VND as 
    float
) as VND,
    cast(VUV as 
    float
) as VUV,
    cast(WST as 
    float
) as WST,
    cast(XAF as 
    float
) as XAF,
    cast(XAG as 
    float
) as XAG,
    cast(XAU as 
    float
) as XAU,
    cast(XCD as 
    float
) as XCD,
    cast(XDR as 
    float
) as XDR,
    cast(XOF as 
    float
) as XOF,
    cast(XPF as 
    float
) as XPF,
    cast(YER as 
    float
) as YER,
    cast(ZAR as 
    float
) as ZAR,
    cast(ZMK as 
    float
) as ZMK,
    cast(ZMW as 
    float
) as ZMW,
    cast(ZWL as 
    float
) as ZWL,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__EXCHANGE_RATES_RATES_AB1
-- RATES at exchange_rates/rates
where 1 = 1

),  __dbt__cte__EXCHANGE_RATES_RATES_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__EXCHANGE_RATES_RATES_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_EXCHANGE_RATES_HASHID as 
    varchar
), '') || '-' || coalesce(cast(AED as 
    varchar
), '') || '-' || coalesce(cast(AFN as 
    varchar
), '') || '-' || coalesce(cast("ALL" as 
    varchar
), '') || '-' || coalesce(cast(AMD as 
    varchar
), '') || '-' || coalesce(cast(ANG as 
    varchar
), '') || '-' || coalesce(cast(AOA as 
    varchar
), '') || '-' || coalesce(cast(ARS as 
    varchar
), '') || '-' || coalesce(cast(AUD as 
    varchar
), '') || '-' || coalesce(cast(AWG as 
    varchar
), '') || '-' || coalesce(cast(AZN as 
    varchar
), '') || '-' || coalesce(cast(BAM as 
    varchar
), '') || '-' || coalesce(cast(BBD as 
    varchar
), '') || '-' || coalesce(cast(BDT as 
    varchar
), '') || '-' || coalesce(cast(BGN as 
    varchar
), '') || '-' || coalesce(cast(BHD as 
    varchar
), '') || '-' || coalesce(cast(BIF as 
    varchar
), '') || '-' || coalesce(cast(BMD as 
    varchar
), '') || '-' || coalesce(cast(BND as 
    varchar
), '') || '-' || coalesce(cast(BOB as 
    varchar
), '') || '-' || coalesce(cast(BRL as 
    varchar
), '') || '-' || coalesce(cast(BSD as 
    varchar
), '') || '-' || coalesce(cast(BTC as 
    varchar
), '') || '-' || coalesce(cast(BTN as 
    varchar
), '') || '-' || coalesce(cast(BWP as 
    varchar
), '') || '-' || coalesce(cast(BYN as 
    varchar
), '') || '-' || coalesce(cast(BYR as 
    varchar
), '') || '-' || coalesce(cast(BZD as 
    varchar
), '') || '-' || coalesce(cast(CAD as 
    varchar
), '') || '-' || coalesce(cast(CDF as 
    varchar
), '') || '-' || coalesce(cast(CHF as 
    varchar
), '') || '-' || coalesce(cast(CLF as 
    varchar
), '') || '-' || coalesce(cast(CLP as 
    varchar
), '') || '-' || coalesce(cast(CNY as 
    varchar
), '') || '-' || coalesce(cast(COP as 
    varchar
), '') || '-' || coalesce(cast(CRC as 
    varchar
), '') || '-' || coalesce(cast(CUC as 
    varchar
), '') || '-' || coalesce(cast(CUP as 
    varchar
), '') || '-' || coalesce(cast(CVE as 
    varchar
), '') || '-' || coalesce(cast(CZK as 
    varchar
), '') || '-' || coalesce(cast(DJF as 
    varchar
), '') || '-' || coalesce(cast(DKK as 
    varchar
), '') || '-' || coalesce(cast(DOP as 
    varchar
), '') || '-' || coalesce(cast(DZD as 
    varchar
), '') || '-' || coalesce(cast(EGP as 
    varchar
), '') || '-' || coalesce(cast(ERN as 
    varchar
), '') || '-' || coalesce(cast(ETB as 
    varchar
), '') || '-' || coalesce(cast(EUR as 
    varchar
), '') || '-' || coalesce(cast(FJD as 
    varchar
), '') || '-' || coalesce(cast(FKP as 
    varchar
), '') || '-' || coalesce(cast(GBP as 
    varchar
), '') || '-' || coalesce(cast(GEL as 
    varchar
), '') || '-' || coalesce(cast(GGP as 
    varchar
), '') || '-' || coalesce(cast(GHS as 
    varchar
), '') || '-' || coalesce(cast(GIP as 
    varchar
), '') || '-' || coalesce(cast(GMD as 
    varchar
), '') || '-' || coalesce(cast(GNF as 
    varchar
), '') || '-' || coalesce(cast(GTQ as 
    varchar
), '') || '-' || coalesce(cast(GYD as 
    varchar
), '') || '-' || coalesce(cast(HKD as 
    varchar
), '') || '-' || coalesce(cast(HNL as 
    varchar
), '') || '-' || coalesce(cast(HRK as 
    varchar
), '') || '-' || coalesce(cast(HTG as 
    varchar
), '') || '-' || coalesce(cast(HUF as 
    varchar
), '') || '-' || coalesce(cast(IDR as 
    varchar
), '') || '-' || coalesce(cast(ILS as 
    varchar
), '') || '-' || coalesce(cast(IMP as 
    varchar
), '') || '-' || coalesce(cast(INR as 
    varchar
), '') || '-' || coalesce(cast(IQD as 
    varchar
), '') || '-' || coalesce(cast(IRR as 
    varchar
), '') || '-' || coalesce(cast(ISK as 
    varchar
), '') || '-' || coalesce(cast(JEP as 
    varchar
), '') || '-' || coalesce(cast(JMD as 
    varchar
), '') || '-' || coalesce(cast(JOD as 
    varchar
), '') || '-' || coalesce(cast(JPY as 
    varchar
), '') || '-' || coalesce(cast(KES as 
    varchar
), '') || '-' || coalesce(cast(KGS as 
    varchar
), '') || '-' || coalesce(cast(KHR as 
    varchar
), '') || '-' || coalesce(cast(KMF as 
    varchar
), '') || '-' || coalesce(cast(KPW as 
    varchar
), '') || '-' || coalesce(cast(KRW as 
    varchar
), '') || '-' || coalesce(cast(KWD as 
    varchar
), '') || '-' || coalesce(cast(KYD as 
    varchar
), '') || '-' || coalesce(cast(KZT as 
    varchar
), '') || '-' || coalesce(cast(LAK as 
    varchar
), '') || '-' || coalesce(cast(LBP as 
    varchar
), '') || '-' || coalesce(cast(LKR as 
    varchar
), '') || '-' || coalesce(cast(LRD as 
    varchar
), '') || '-' || coalesce(cast(LSL as 
    varchar
), '') || '-' || coalesce(cast(LTL as 
    varchar
), '') || '-' || coalesce(cast(LVL as 
    varchar
), '') || '-' || coalesce(cast(LYD as 
    varchar
), '') || '-' || coalesce(cast(MAD as 
    varchar
), '') || '-' || coalesce(cast(MDL as 
    varchar
), '') || '-' || coalesce(cast(MGA as 
    varchar
), '') || '-' || coalesce(cast(MKD as 
    varchar
), '') || '-' || coalesce(cast(MMK as 
    varchar
), '') || '-' || coalesce(cast(MNT as 
    varchar
), '') || '-' || coalesce(cast(MOP as 
    varchar
), '') || '-' || coalesce(cast(MRO as 
    varchar
), '') || '-' || coalesce(cast(MUR as 
    varchar
), '') || '-' || coalesce(cast(MVR as 
    varchar
), '') || '-' || coalesce(cast(MWK as 
    varchar
), '') || '-' || coalesce(cast(MXN as 
    varchar
), '') || '-' || coalesce(cast(MYR as 
    varchar
), '') || '-' || coalesce(cast(MZN as 
    varchar
), '') || '-' || coalesce(cast(NAD as 
    varchar
), '') || '-' || coalesce(cast(NGN as 
    varchar
), '') || '-' || coalesce(cast(NIO as 
    varchar
), '') || '-' || coalesce(cast(NOK as 
    varchar
), '') || '-' || coalesce(cast(NPR as 
    varchar
), '') || '-' || coalesce(cast(NZD as 
    varchar
), '') || '-' || coalesce(cast(OMR as 
    varchar
), '') || '-' || coalesce(cast(PAB as 
    varchar
), '') || '-' || coalesce(cast(PEN as 
    varchar
), '') || '-' || coalesce(cast(PGK as 
    varchar
), '') || '-' || coalesce(cast(PHP as 
    varchar
), '') || '-' || coalesce(cast(PKR as 
    varchar
), '') || '-' || coalesce(cast(PLN as 
    varchar
), '') || '-' || coalesce(cast(PYG as 
    varchar
), '') || '-' || coalesce(cast(QAR as 
    varchar
), '') || '-' || coalesce(cast(RON as 
    varchar
), '') || '-' || coalesce(cast(RSD as 
    varchar
), '') || '-' || coalesce(cast(RUB as 
    varchar
), '') || '-' || coalesce(cast(RWF as 
    varchar
), '') || '-' || coalesce(cast(SAR as 
    varchar
), '') || '-' || coalesce(cast(SBD as 
    varchar
), '') || '-' || coalesce(cast(SCR as 
    varchar
), '') || '-' || coalesce(cast(SDG as 
    varchar
), '') || '-' || coalesce(cast(SEK as 
    varchar
), '') || '-' || coalesce(cast(SGD as 
    varchar
), '') || '-' || coalesce(cast(SHP as 
    varchar
), '') || '-' || coalesce(cast(SLL as 
    varchar
), '') || '-' || coalesce(cast(SOS as 
    varchar
), '') || '-' || coalesce(cast(SRD as 
    varchar
), '') || '-' || coalesce(cast(STD as 
    varchar
), '') || '-' || coalesce(cast(SVC as 
    varchar
), '') || '-' || coalesce(cast(SYP as 
    varchar
), '') || '-' || coalesce(cast(SZL as 
    varchar
), '') || '-' || coalesce(cast(THB as 
    varchar
), '') || '-' || coalesce(cast(TJS as 
    varchar
), '') || '-' || coalesce(cast(TMT as 
    varchar
), '') || '-' || coalesce(cast(TND as 
    varchar
), '') || '-' || coalesce(cast(TOP as 
    varchar
), '') || '-' || coalesce(cast(TRY as 
    varchar
), '') || '-' || coalesce(cast(TTD as 
    varchar
), '') || '-' || coalesce(cast(TWD as 
    varchar
), '') || '-' || coalesce(cast(TZS as 
    varchar
), '') || '-' || coalesce(cast(UAH as 
    varchar
), '') || '-' || coalesce(cast(UGX as 
    varchar
), '') || '-' || coalesce(cast(USD as 
    varchar
), '') || '-' || coalesce(cast(UYU as 
    varchar
), '') || '-' || coalesce(cast(UZS as 
    varchar
), '') || '-' || coalesce(cast(VEF as 
    varchar
), '') || '-' || coalesce(cast(VND as 
    varchar
), '') || '-' || coalesce(cast(VUV as 
    varchar
), '') || '-' || coalesce(cast(WST as 
    varchar
), '') || '-' || coalesce(cast(XAF as 
    varchar
), '') || '-' || coalesce(cast(XAG as 
    varchar
), '') || '-' || coalesce(cast(XAU as 
    varchar
), '') || '-' || coalesce(cast(XCD as 
    varchar
), '') || '-' || coalesce(cast(XDR as 
    varchar
), '') || '-' || coalesce(cast(XOF as 
    varchar
), '') || '-' || coalesce(cast(XPF as 
    varchar
), '') || '-' || coalesce(cast(YER as 
    varchar
), '') || '-' || coalesce(cast(ZAR as 
    varchar
), '') || '-' || coalesce(cast(ZMK as 
    varchar
), '') || '-' || coalesce(cast(ZMW as 
    varchar
), '') || '-' || coalesce(cast(ZWL as 
    varchar
), '') as 
    varchar
)) as _AIRBYTE_RATES_HASHID,
    tmp.*
from __dbt__cte__EXCHANGE_RATES_RATES_AB2 tmp
-- RATES at exchange_rates/rates
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__EXCHANGE_RATES_RATES_AB3
select
    _AIRBYTE_EXCHANGE_RATES_HASHID,
    AED,
    AFN,
    "ALL",
    AMD,
    ANG,
    AOA,
    ARS,
    AUD,
    AWG,
    AZN,
    BAM,
    BBD,
    BDT,
    BGN,
    BHD,
    BIF,
    BMD,
    BND,
    BOB,
    BRL,
    BSD,
    BTC,
    BTN,
    BWP,
    BYN,
    BYR,
    BZD,
    CAD,
    CDF,
    CHF,
    CLF,
    CLP,
    CNY,
    COP,
    CRC,
    CUC,
    CUP,
    CVE,
    CZK,
    DJF,
    DKK,
    DOP,
    DZD,
    EGP,
    ERN,
    ETB,
    EUR,
    FJD,
    FKP,
    GBP,
    GEL,
    GGP,
    GHS,
    GIP,
    GMD,
    GNF,
    GTQ,
    GYD,
    HKD,
    HNL,
    HRK,
    HTG,
    HUF,
    IDR,
    ILS,
    IMP,
    INR,
    IQD,
    IRR,
    ISK,
    JEP,
    JMD,
    JOD,
    JPY,
    KES,
    KGS,
    KHR,
    KMF,
    KPW,
    KRW,
    KWD,
    KYD,
    KZT,
    LAK,
    LBP,
    LKR,
    LRD,
    LSL,
    LTL,
    LVL,
    LYD,
    MAD,
    MDL,
    MGA,
    MKD,
    MMK,
    MNT,
    MOP,
    MRO,
    MUR,
    MVR,
    MWK,
    MXN,
    MYR,
    MZN,
    NAD,
    NGN,
    NIO,
    NOK,
    NPR,
    NZD,
    OMR,
    PAB,
    PEN,
    PGK,
    PHP,
    PKR,
    PLN,
    PYG,
    QAR,
    RON,
    RSD,
    RUB,
    RWF,
    SAR,
    SBD,
    SCR,
    SDG,
    SEK,
    SGD,
    SHP,
    SLL,
    SOS,
    SRD,
    STD,
    SVC,
    SYP,
    SZL,
    THB,
    TJS,
    TMT,
    TND,
    TOP,
    TRY,
    TTD,
    TWD,
    TZS,
    UAH,
    UGX,
    USD,
    UYU,
    UZS,
    VEF,
    VND,
    VUV,
    WST,
    XAF,
    XAG,
    XAU,
    XCD,
    XDR,
    XOF,
    XPF,
    YER,
    ZAR,
    ZMK,
    ZMW,
    ZWL,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_RATES_HASHID
from __dbt__cte__EXCHANGE_RATES_RATES_AB3

