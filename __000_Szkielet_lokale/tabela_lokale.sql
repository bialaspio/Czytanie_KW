DROP TABLE IF EXISTS kw_lok_Kamienica_tmp1;
create table kw_lok_Kamienica_tmp1 AS 
select nr_kw,NR_KW_BUD,(select wojewodztwonazwa||' '||powiatnazwa||' '||gminanazwa||' '||nazwa_miej from polozenie_Kamienica B where A.nr_kw like B.nr_kw) as polozenie, ulica, kw_nr_bud, kw_nr_lok , kw_kond, (select kw_pow from lokale_pow_Kamienica C where A.nr_kw like C.nr_kw ) as pow_lok, kw_typ_lok, kw_licz_izb, pom_przyn,(select string_agg(D.nr_udzialu,'|') from udzialy_lokale_Kamienica D where A.nr_kw = D.nr_kw)as nr_udzialu from lokale_Kamienica A;

DROP TABLE IF EXISTS kw_lok_Kamienica_tmp2;
create table kw_lok_Kamienica_tmp2 AS 
SELECT A.*, 
(select string_agg(B.imie||' '||B.nazwisko_1||' '||B.nazwisko_2,';') from wlasciciele_lokale_Kamienica B where A.nr_kw = B.nr_kw and B.nr_udzialu in (select unnest(string_to_array(A.nr_udzialu, '|')))) as wlasciciele,
(select string_agg(B.ojciec||' '||B.marka,';') from wlasciciele_lokale_Kamienica B where A.nr_kw = B.nr_kw and B.nr_udzialu in (select unnest(string_to_array(A.nr_udzialu, '|')))) as rodzice
FROM kw_lok_Kamienica_tmp1 A; 

drop table if exists kw_lok_Kamienica_tmp3;
create table kw_lok_Kamienica_tmp3 as
select nr_kw, pom_przyn, unnest(string_to_array(pom_przyn, '|')), substring (upper (replace (unnest(string_to_array(pom_przyn, '|')),' ','')),'[0-9]{1,3}.[0-9]{0,3}M2') as pow_lok from  kw_lok_Kamienica_tmp2  where pom_przyn not like '' and upper (replace (pom_przyn,' ','')) like '%M2%';

drop table if exists kw_lok_Kamienica;
create table kw_lok_Kamienica as 
select 
nr_kw,''::varchar as nr_dzialki,''::varchar as nr_dzialki_2, NR_KW_BUD, ''::varchar id_bud, polozenie, ulica, kw_nr_bud, kw_nr_lok, kw_kond, pow_lok, kw_typ_lok, kw_licz_izb, pom_przyn, 
(select string_agg(pow_lok, '|') from kw_lok_Kamienica_tmp3 B where A.nr_kw=B.nr_kw) as pol_pow_pom_przyn,''::varchar as ID_bud_pom_przyn,''::varchar as Wyr_LN_LI, ''::varchar as Prawo_wsp_do_bud,''::varchar as prawo_wieczyste, 
nr_udzialu, wlasciciele, rodzice
from 
kw_lok_Kamienica_tmp2 A;

update kw_lok_Kamienica set kw_typ_lok = 1 where kw_typ_lok like 'LOKAL MIESZKALNY';
update kw_lok_Kamienica set kw_typ_lok = 2 where kw_typ_lok like 'LOKAL NIEMIESZKALNY';

UPDATE kw_lok_Kamienica SET WLASCICIELE = REPLACE (WLASCICIELE, '- - -','') WHERE WLASCICIELE LIKE '%- - -%';
      

--alter table kw_lok_Kamienica drop if exists nr_udzialu;

select *from kw_lok_Kamienica

select distinct nr_kw from kw_lok_Kamienica
