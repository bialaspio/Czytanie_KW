import BeautifulSoup
import sys,glob,os,shutil,time, datetime 
import xml.etree.ElementTree as ET
import re
import lxml.etree
import lxml.builder    
import psycopg2 


#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~

def exec_query_PG ( str_query ):
	try:
		cur_PG.execute(str_query) 
		print "Wykonane zapytanie "+str_query
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		err_log.write('Nie udalo sie wykonac zapytania (select):'+str_query+'\n')
		print e
	return cur_PG

#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~

def exec_query_commit_PG( str_query ):
	try:
		cur_PG.execute(str_query) 
		conn_PG.commit()
		print "Wykonane zapytanie "+str_query
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		print e
		err_log.write('Nie udalo sie wykonac zapytania (commit):'+str_query+'\n')
		os.system("pause")
	return cur_PG 	

#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~




def get_files_txt():
	fn = []
	for (dirpath, dirnames, filenames) in os.walk(main_dir):
		fn.extend(filenames)
		for element in filenames:
			#try:
			#print element
			if element[-4:]=='html' or element[-3:]=='HTML':
				old_name_path = dirpath+'\\'+element
				print old_name_path 
				html2xml(old_name_path) 
				#os.system("pause")


def html2xml (old_name_path):
	f = open(old_name_path)
	soup = BeautifulSoup.BeautifulSoup(f)
	f.close()
	xml_file = old_name_path[:-4]
	xml_file_name = xml_file+'xml'
	g = open(xml_file_name , 'w')
	print >> g, soup.prettify()
	g.close()
	get_data_from_xml(xml_file_name)

def get_data_from_xml(xml_file_name):
	
	if os.path.exists(xml_file_name)==True: 
		tree = ET.parse(xml_file_name)
		root = tree.getroot()
		for body in root.findall('body'):
			f_nr_ksw = 0
			f_pow_lok = 0
			insert_miej = 0
			for table in body.findall('table'):
				LP_polozenie = 0
				th_tabelka = 'Brak'
				for tr in table.findall('tr'):
					
					for th in tr.findall('th'):
						th_tabelka = th.text.strip()
						#os.system("pause")
						
					if 'Rubryka 1.3 - Po' in th_tabelka:
						for td in tr.findall('td'):
							ATRYB_TD = td.attrib
							TD_ID = ATRYB_TD.get('id')
							TD_ROWSPAN = ATRYB_TD.get('rowspan')
							print th_tabelka
							#BB1B-00015282-5.html

							#----------------------------------------------------------------
							# Polozenie
							#----------------------------------------------------------------
							#LP
							print insert_miej
							#os.system("pause")
							if TD_ROWSPAN == '6':
								if LP_polozenie == 0:
									LP_polozenie = td.text.strip()
									if LP_polozenie =='':
										LP_polozenie ='PUSTE'
									print '\tLP pozdkowy miejscowosci: '+ LP_polozenie
									insert_miej = insert_miej+1	
							
							#Numer pozadkowy 
							if TD_ID == 'NumerPorzadkowy':
								nr_poz_miej = td.text.strip()
								if nr_poz_miej =='':
									nr_poz_miej ='PUSTE'
								print '\tNR pozdkowy miejscowosci: '+ nr_poz_miej
								insert_miej = insert_miej+10
								
							#WojewodztwoNazwa
							if TD_ID == 'WojewodztwoNazwa':
								WojewodztwoNazwa = td.text.strip()
								if WojewodztwoNazwa =='':
									WojewodztwoNazwa ='PUSTE'
								print '\tNazwa wojewodztwa: '+ WojewodztwoNazwa
								insert_miej = insert_miej+100

							#PowiatNazwa							
							if TD_ID == 'PowiatNazwa':
								PowiatNazwa = td.text.strip()
								if PowiatNazwa =='':
									PowiatNazwa ='PUSTE'
								print '\tNazwa powiatu: '+ PowiatNazwa
								insert_miej = insert_miej+1000
								
							#GminaNazwa
							if TD_ID == 'GminaNazwa':
								GminaNazwa = td.text.strip()
								if GminaNazwa =='':
									GminaNazwa ='PUSTE'
								print '\tNazwa Gmina: '+ GminaNazwa
								insert_miej = insert_miej+10000
								
							
							#mijscowosc nazwa 
							if TD_ID == 'MiejscowoscNazwa':
								miej_nazwa = td.text.strip()
								if miej_nazwa =='':
									miej_nazwa ='PUSTE'
								print '\tNazwa miejscowosci: '+ miej_nazwa
								insert_miej = insert_miej+100000
							
							
							if insert_miej == 111111:
								insert_query = "insert into polozenie_Kamienica(NR_KW,LP_polozenie,WojewodztwoNazwa,PowiatNazwa,GminaNazwa,nazwa_miej,nr_poz) VALUES ('"+nr_ksw+"','"+LP_polozenie+"','"+WojewodztwoNazwa+"','"+PowiatNazwa+"','"+GminaNazwa+"','"+miej_nazwa+"',"+ nr_poz_miej +"::int)"  
								try: 
									exec_query_commit_PG(insert_query)
									insert_miej = 0
									LP_polozenie = 0
								except:
									print "ERR - " +insert_query
									os.system("pause")
					
					
					
					for td in tr.findall('td'):
						ATRYB_TD = td.attrib
						TD_ID = ATRYB_TD.get('id')
						
						#Numer ksiegi	
						if 'Numer ksiegi' in td.text:
							licznik = 1
							for td_lokal in tr:
								if licznik == 3:
									nr_ksw = td_lokal.text.strip()
									print 'Nr KSW : '+ nr_ksw
									f_nr_ksw = 1
								licznik = licznik+1		
						
						if '1. Obszar' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									pow_lok= td_lokal.text.strip()
									print '\t1. Obszar '+ pow_lok
									f_pow_lok = 1
								licznik = licznik + 1
						
						if f_nr_ksw ==1 and f_pow_lok == 1:
							insert_query = "insert into lokale_pow_Kamienica(NR_KW,KW_pow ) VALUES ('"+str(nr_ksw)+"','"+str( pow_lok)+"');"
							exec_query_commit_PG(insert_query)
							f_pow_lok = 0
						
						#---- wielkosc udzialu 
						#nr udzialu 
						if '1. Numer udzia' in td.text:
							licznik = 1
							for td_lokal in tr:
								if licznik == 4:
									nr_udzialu = td_lokal.text.strip()
									print 'Numer udzialu: '+ nr_udzialu
								licznik = licznik+1		

						#wielkosc udzialu 
						if 'u (licznik/mianownik)' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									wielkosc_udzialu = td_lokal.text.strip()
#									if wielkosc_udzialu == '1/1':
#										nr_udzialu = '1'
									print 'wielkosc_udzialu: '+ wielkosc_udzialu
									insert_query = "insert into udzialy_lokale_Kamienica(NR_KW, NR_udzialu,Udzial) VALUES ('"+nr_ksw +"','"+nr_udzialu+"','"+wielkosc_udzialu+"')"
									exec_query_commit_PG(insert_query)						

								licznik = licznik+1		
								
						#---- imie i nazwako wlasciciela 
						
						#numer udzialu 
						for table1 in td.findall('table'):
#							print '6'
							nr_udzialu_oso = ''
							insert_osoba = 0
							for tr1 in table1.findall('tr'):
#								print '7'
								licznik = 1
								for td1 in tr1.findall('td'):
									#print '8'
									#print td1.attrib
									if licznik == 3:
										nr_udzialu_oso = td1.text.strip()
										print 'Numer nr_udzialu_oso: '+ nr_udzialu_oso
									licznik = licznik+1	
						
						
						#pierwsze imie 
						if '2. Imi' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									pierw_imie = td_lokal.text.strip()
									print 'Pierwsze imie: '+ pierw_imie
									insert_osoba = insert_osoba+1
									#print insert_osoba
								licznik = licznik+1
							if licznik < 4:
								insert_osoba = 0
						
						#drugie imie 
						if '3. Imi' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									drugie_imie = td_lokal.text.strip()
									print 'Drugie imie: '+ drugie_imie
									insert_osoba = insert_osoba+10
									#print insert_osoba
								licznik = licznik+1	
							if licznik < 4:
								insert_osoba = 0								
								
						#nazwisko pierwszy czlon 
						if '4. Nazwisko / pierwszy' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									nazwisko_pierw_cz= td_lokal.text.strip()
									print 'Nazwisko 1: '+ nazwisko_pierw_cz
									insert_osoba = insert_osoba+100
									#print insert_osoba
								licznik = licznik+1		
							if licznik < 4:
								insert_osoba = 0								

						#nazwisko pierwszy czlon 
						if '5. Drugi cz' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									nazwisko_drugi_cz = td_lokal.text.strip()
									print 'Nazwisko 2: '+ nazwisko_drugi_cz
									insert_osoba = insert_osoba+1000
									#print insert_osoba
								licznik = licznik+1		
							if licznik < 4:
								insert_osoba = 0								
						#Imie ojca 
						if '6. Imi' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									imie_ojca = td_lokal.text.strip()
									print 'Imie ojca :'+ imie_ojca
									insert_osoba = insert_osoba+10000
									#print insert_osoba
								licznik = licznik+1
							if licznik < 4:
								insert_osoba = 0						
						
						#Imie matki
						if '7. Imi' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									imie_matki = td_lokal.text.strip()
									print 'Imie matki:'+ imie_matki
									insert_osoba = insert_osoba+100000
									#print insert_osoba
								licznik = licznik+1
							if licznik < 4:
								insert_osoba = 0	

						#PESEL
						if '8. PESEL' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									PESEL = td_lokal.text.strip()
									print 'PESEL :'+ PESEL
									print insert_osoba	
									if insert_osoba==111111:
										insert_query = "insert into wlasciciele_lokale_Kamienica(NR_KW, NR_udzialu,Imie,Drugie_imie,Nazwisko_1,Nazwisko_2,Ojciec,Marka, PESEL) VALUES ('"+nr_ksw +"','"+nr_udzialu_oso+"','"+pierw_imie+"','"+drugie_imie+"','"+nazwisko_pierw_cz+"','"+nazwisko_drugi_cz+"','"+imie_ojca+"','"+imie_matki+"','"+PESEL+"')" 
										exec_query_commit_PG(insert_query)						
										insert_osoba = 0
								licznik = licznik+1																
						
						
						
						#print insert_osoba 
						#instytucja 
						if '2. Nazwa' in td.text:
							licznik = 1
							for td_lokal in tr:								
								if licznik == 3:
									instytucja = td_lokal.text.strip()
									print 'Instytucja: '+ instytucja
									insert_query = "insert into wlasciciele_lokale_Kamienica(NR_KW, NR_udzialu,Imie,Drugie_imie,Nazwisko_1,Nazwisko_2,Ojciec,Marka) VALUES ('"+nr_ksw +"','"+nr_udzialu_oso+"','"+instytucja+"','','','','','')"
									exec_query_commit_PG(insert_query)						
								licznik = licznik+1	
				
				
				ATRYB_TABLE = table.attrib
				TABLE_ID = ATRYB_TABLE.get('id')
				if TABLE_ID == 'Lokale': 
					for tbody in table.findall('tbody'):
						ATRYB_TBODY = tbody.attrib
						TBODY_ID = ATRYB_TBODY.get('id')
						if TBODY_ID == "LokalKW":
							licz_pom = 0
							for t_tr in tbody.findall('tr'):
								for t_td in t_tr.findall('td'):
									ATRYB_TD = t_td.attrib
									TD_ID = ATRYB_TD.get('id')
									
									if '2. Ulica' in t_td.text:
										licznik = 1 
										for td_lokal in t_tr:								
											if licznik == 3:
												Ulica = td_lokal.text.strip()
												#print '\t3. Ulica '+ nr_bud
											licznik = licznik + 1
									
									if '3. Numer budynku' in t_td.text:
										licznik = 1 
										for td_lokal in t_tr:								
											if licznik == 3:
												nr_bud = td_lokal.text.strip()
												print '\t3. Numer budynku '+ nr_bud
											licznik = licznik + 1
									                     
									if '4. Numer lokalu' in t_td.text:
										licznik = 1 
										for td_lokal in t_tr:								
											if licznik == 3:
												nr_lokal = td_lokal.text.strip()
												print '\t4. Numer lokalu '+ nr_lokal
											licznik = licznik + 1
											       
									if '5. Przeznaczenie lokalu' in t_td.text:
										licznik = 1
										for td_lokal in t_tr:								
											if licznik == 3:
												przez_lokal = td_lokal.text.strip()
												print '\t5. Przeznaczenie lokalu '+ przez_lokal
											licznik = licznik + 1		
									
									if '6. Opis lokalu' in t_td.text:
										licz_lok = 0
										licz_lok_tmp = 0
										for td_lokal in t_tr:
											for table_licz_pom in td_lokal.findall('table'):
												for tr_licz_pom in table_licz_pom.findall('tr'):
													for td_licz_pom in tr_licz_pom.findall('td'):
														if 'B: liczba izb' in td_licz_pom.text:
															licznik = 1
															for td_lokal_licz_pom in tr_licz_pom:
																if licznik == 3:
																	#print td_lokal_licz_pom.text.strip() 
																	try: 
																		licz_lok_tmp = int(td_lokal_licz_pom.text.strip())
																	except:
																		licz_lok_tmp = 0
																	licz_lok = licz_lok+licz_lok_tmp
																licznik = licznik + 1
										print 'licz_lok - '+str (licz_lok)
									
									#7. Opis pomieszc
									if '7. Opis pomieszc' in t_td.text:
										
										for td_lokal in t_tr:
											pom_przyn=''
											for table_pom_przyn in td_lokal.findall('table'):
												
												for tr_pom_przyn in table_pom_przyn.findall('tr'):
													
													for td_pom_przyn in tr_pom_przyn.findall('td'):
														
														if 'A: rodzaj pomieszczenia' in td_pom_przyn.text:
															
															licznik = 1
															for td_lokal_pom_przyn in tr_pom_przyn:
																if licznik == 4:
																	pom_przyn = pom_przyn +'|'+td_lokal_pom_przyn.text.strip()
																licznik = licznik + 1
										#print 'pom_przyn - '+str (pom_przyn)
										
										
									if '8. Kondygnacja' in t_td.text:
										licznik = 1
										for td_lokal in t_tr:								
											if licznik == 3:
												kondygnacja = td_lokal.text.strip()
												print '\t8. Kondygnacja '+ kondygnacja
											licznik = licznik + 1
											
									#9. Przylaczenie - numer ksiegi wieczystej
									if '9. Przy' in t_td.text:
										nr_kw_bud =''
										licznik = 1
										for td_lokal in t_tr:
											for table_przyl in td_lokal.findall('table'):
												for tr_przyl in table_przyl.findall('tr'):
													for td_przyl in tr_przyl.findall('td'):
														if licznik == 3: 
															nr_kw_bud = td_przyl.text.strip()
														licznik = licznik + 1

									 		

#						print str(nr_ksw)
#						print str(nr_bud)
#						print str( nr_kw_bud)
#						print str( przez_lokal)
#						print str(licz_lok)
#						print str(kondygnacja)
#						print Ulica
#						print nr_lokal
#						print pom_przyn

						insert_query = "insert into lokale_Kamienica(NR_KW,nr_kw_bud,Ulica, KW_nr_bud, KW_nr_lok, KW_typ_lok, KW_licz_izb,pom_przyn,KW_kond) VALUES ('"+str(nr_ksw)+"','"+nr_kw_bud+"','"+Ulica+"','"+str(nr_bud)+"','"+nr_lokal+"','"+str( przez_lokal)+"','"+str(licz_lok)+"','"+pom_przyn+ "','"+str(kondygnacja)+"');"
						
						exec_query_commit_PG(insert_query)
						
				

				
					
def creae_tables ():
	# dziala z numerem i powierzchnia 
	drop_quer = "drop table if exists lokale_Kamienica;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table lokale_Kamienica(
		NR_KW varchar,
		NR_KW_BUD varchar,
		Ulica varchar,
		KW_nr_bud varchar, 
		KW_nr_lok varchar,
		KW_typ_lok varchar, 
		KW_licz_izb varchar,
		pom_przyn varchar,		
		KW_kond varchar
	);"""
	exec_query_commit_PG(create_query)

#Powierzchnia lokalow  
	drop_quer = "drop table if exists lokale_pow_Kamienica;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table lokale_pow_Kamienica(
		NR_KW varchar,
		KW_pow varchar
		);
	"""
	exec_query_commit_PG(create_query)
	
#Wlasciciel z rodzice 
	drop_quer = "drop table if exists wlasciciele_lokale_Kamienica;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table wlasciciele_lokale_Kamienica (
		NR_KW varchar, 
		NR_udzialu varchar, 	
		Imie varchar,
		Drugie_imie varchar,
		Nazwisko_1 varchar,
		Nazwisko_2 varchar,
		Ojciec varchar,
		Marka varchar,
		PESEL varchar
	);"""
	exec_query_commit_PG(create_query)

	#udzialy 
	drop_quer = "drop table if exists udzialy_lokale_Kamienica;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table udzialy_lokale_Kamienica (
		NR_KW varchar, 
		NR_udzialu varchar, 	
		Udzial varchar
	);"""
	exec_query_commit_PG(create_query)

	#NR_KW,WojewodztwoNazwa,PowiatNazwa,GminaNazwa,nazwa_miej,nr_poz
	drop_quer = "drop table if exists polozenie_Kamienica;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table polozenie_Kamienica (
	 NR_KW varchar,
	 LP_polozenie varchar,
	 WojewodztwoNazwa varchar,
	 PowiatNazwa varchar,
	 GminaNazwa varchar,
	 nazwa_miej varchar,
	 nr_poz int
	);"""
	exec_query_commit_PG(create_query)


#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
##############################################################################################################################################################
#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
	
if not os.path.exists('.\\log'):
    try:
        os.makedirs('.\\log')
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

naz_lik_log = time.strftime("%Y%m%d-%H_%M_%S")

naz_log = open('.\\log\\copy'+naz_lik_log+'.log', 'w')
err_log = open('.\\log\\ERR'+naz_lik_log+'.log', 'w')
csv_file = open('.\\csv_'+naz_lik_log+'.csv', 'w')		
csv_file2 = open('.\\csv2_'+naz_lik_log+'.csv', 'w')		
	
main_dir_list = ['C:\\PB\\M_Bielsko\\KW\\lokale\\09_Kamienica\\kw_lokale\\']

#polacznie z baza POSTGRES 
#teraz = time.asctime( time.lokaltime(time.time()))
#naz_log.write('START -' +teraz + '\n')
try:
	conn_PG = psycopg2.connect("dbname='kw_Kamienica_lokale_20190917' user='postgres' host='crait' password='aaaaaa'")
	naz_log.write(' [INF] Polaczono z baza danych\n')
except:
	print "I am unable to connect to the database"
	err_log.write(' [ERR] Nie udalo sie naiazac polacznia z baz\n')

cur_PG = conn_PG.cursor()
E = lxml.builder.ElementMaker()
creae_tables()
for main_dir in main_dir_list:
	print main_dir 
	get_files_txt()
