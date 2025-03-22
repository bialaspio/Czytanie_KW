-- dziski z budynkami 
drop table if exists tmp1;
create table tmp1 as 
select 
(select B.nr_dzialki from dzialki_Pow_KR_7 B where split_part(A.ident_dzi,'.',3) like B.nr_dzialki) as nr_dzialki,
(select B.Numerobrebuewid||'-'||B.nazwaobrebuewid from dzialki_Pow_KR_7 B where split_part(A.ident_dzi,'.',3) like B.nr_dzialki) as obreb,
(select B.pol_dzialki from dzialki_Pow_KR_7 B where split_part(A.ident_dzi,'.',3) like B.nr_dzialki) as pol_dzialki,
A.* from budynki_Pow_KR_7 A;


drop table if exists tmp2;
create table tmp2 as 
select 
(select B.wojewodztwonazwa ||';'|| B.powiatnazwa ||';'|| B.gminanazwa ||';'||B.nazwa_miej from polozenie_Pow_KR_7 B where A.nr_kw = B.nr_kw and 
B.nr_poz::varchar = A.pol_dzialki) as polozenie,
(select string_agg(B.nr_udzialu,'|') from udzialy_Pow_KR_7 B where A.nr_kw = B.nr_kw)as nr_udzialu,
(select C.pow_sum_dzialek from pow_sum_dzial_Pow_KR_7 C where A.nr_kw = C.nr_kw )as obszar,
A.* from tmp1 A;

drop table if exists tmp3;
create table tmp3 as 
select 
(select string_agg(B.imie||' '||B.nazwisko_1,';') from wlasciciele_Pow_KR_7 B where A.nr_kw = B.nr_kw and B.nr_udzialu in (select unnest(string_to_array(A.nr_udzialu, '|')))) as wlasciciele,
(select string_agg(B.ojciec||' '||B.marka,';') from wlasciciele_Pow_KR_7 B where A.nr_kw = B.nr_kw and B.nr_udzialu in (select unnest(string_to_array(A.nr_udzialu, '|')))) as rodzice,
A.* from tmp2 A;

drop table if exists dzialki_z_budynkami;
create table dzialki_z_budynkami as 
select 
nr_kw,nr_dzialki , obreb,polozenie,obszar,  identyfikatorbudynku_par01||'-'|| identyfikatorbudynku_par02 as ident_bud, ulica,nrporzadkowy as nr_adr, przeznaczeniebudynku,lokalewyodrebnionenr,lokalewyodrebnionekw,''::varchar as praw_wlas,''::varchar as praw_uzyt_wiecz, wlasciciele,rodzice
from tmp3 ;

--********************************************************************************************************************
-- Dzialki
drop table if exists  tmp1_dzialki;
create table tmp1_dzialki as 
select nr_kw as nr_kw, nr_dzialki as nr_dzialki, Numerobrebuewid||' - '||nazwaobrebuewid as obreb_ewid,
(select B.wojewodztwonazwa ||';'|| B.powiatnazwa ||';'|| B.gminanazwa ||';'||B.nazwa_miej from polozenie_Pow_KR_7 B where A.nr_kw = B.nr_kw and B.nr_poz::varchar = A.pol_dzialki) as polozenie,
(select C.pow_sum_dzialek from pow_sum_dzial_Pow_KR_7 C where A.nr_kw = C.nr_kw )as obszar,
'' as ident_bud,
'' as ulica,
'' as nr_adr,
(select string_agg(B.nr_udzialu,'|') from udzialy_Pow_KR_7 B where A.nr_kw = B.nr_kw)as nr_udzialu
from dzialki_Pow_KR_7 A ;

drop table if exists tmp2_dzialki;
create table tmp2_dzialki as 
select A.*, 
(select string_agg(B.imie||' '||B.nazwisko_1,';') from wlasciciele_Pow_KR_7 B where A.nr_kw = B.nr_kw and B.nr_udzialu in (select unnest(string_to_array(A.nr_udzialu, '|')))) as wlasciciele,
(select string_agg(B.ojciec||' '||B.marka,';') from wlasciciele_Pow_KR_7 B where A.nr_kw = B.nr_kw and B.nr_udzialu in (select unnest(string_to_array(A.nr_udzialu, '|')))) as rodzice
from tmp1_dzialki A;


drop table if exists tabela_dzilaki;
create table tabela_dzilaki as 
select 
nr_kw,nr_dzialki,obreb_ewid as obreb,polozenie,obszar, ident_bud::varchar, ulica::varchar, nr_adr::varchar, ''::varchar as przeznaczeniebudynku,''::varchar as lokalewyodrebnionenr, ''::varchar as lokalewyodrebnionekw,''::varchar as praw_wlas,''::varchar as praw_uzyt_wiecz,  wlasciciele,rodzice
from tmp2_dzialki;

--********************************************************************************************************************
-- Tabela wyjsciowa 
--********************************************************************************************************************
drop table if exists tabela_dzilaki_tmp0;
create table tabela_dzilaki_tmp0 as 
select * from tabela_dzilaki where nr_kw||nr_dzialki not in 
(select distinct B.nr_kw||B.nr_dzialki from tabela_dzilaki A, dzialki_z_budynkami B where A.nr_kw =b.nr_kw and A.nr_dzialki=B.nr_dzialki)
union all 
select *from dzialki_z_budynkami;

drop table if exists tabela_dzilaki_tmp01;
create table tabela_dzilaki_tmp01 as 
select A.*, 
(select string_agg(wiel_udzialu_bud,';') from udz_lok_bud_Pow_KR_7 B where A.nr_kw = B.nr_kw and B.nr_kw_bud in (select unnest(string_to_array(A.lokalewyodrebnionekw, '|')))) as udzialy
from tabela_dzilaki_tmp0 A;

alter table tabela_dzilaki_tmp01 drop if exists ogc_fid;
alter table tabela_dzilaki_tmp01 add ogc_fid serial;

drop table if exists tabela_dzilaki_out;
create table tabela_dzilaki_out as select  
ogc_fid, nr_kw, nr_dzialki, obreb, polozenie, obszar, ident_bud, ulica, nr_adr, przeznaczeniebudynku, lokalewyodrebnionenr, lokalewyodrebnionekw,udzialy as praw_wlas, praw_uzyt_wiecz, wlasciciele, rodzice
from tabela_dzilaki_tmp01;


--********************************************************************************************************************
-- Tabele z tym co nie wszystko

 weszlo i gdzie moga byc bledy  	
--********************************************************************************************************************

"BB1B/00027902/5" - nie udalo sie poporawnie odczytac 


update tabela_dzilaki_out set przeznaczeniebudynku = NULL where przeznaczeniebudynku like 'test' 

select distinct nr_kw from tabela_dzilaki_out;



-- budynki bedziałek
select distinct nr_kw from tabela_dzilaki_out where nr_dzialki is null;
'BB1B/00021134/8','BB1B/00007546/5','BB1B/00063545/8','BB1B/00029079/0','BB1B/00061731/5','BB1B/00027819/6','BB1B/00030795/5','BB1B/00037238/2','BB1B/00035926/8','BB1B/00028940/0','BB1B/00054760/5','BB1B/00035829/8','BB1B/00031930/1','BB1B/00060722/2','BB1B/00072622/8','BB1B/00030657/6','BB1B/00027509/0','BB1B/00067664/6','BB1B/00021208/8','BB1B/00030762/5','BB1B/00020707/9','BB1B/00004987/7','BB1B/00038567/4','BB1B/00044658/4','BB1B/00044053/3','BB1B/00036551/5','BB1B/00083872/5','BB1B/00029264/4','BB1B/00025697/0','BB1B/00008477/7','BB1B/00061068/6','BB1B/00067063/3','BB1B/00007559/9','BB1B/00029262/0','BB1B/00035913/4' - KW w ktorych sa budynki nie przypisane do dzialek 


