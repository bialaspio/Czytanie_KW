import BeautifulSoup
import sys,glob,os,shutil,time
import xml.etree.ElementTree as ET
import re
import lxml.etree
import lxml.builder    
import psycopg2 


#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~

def exec_query_PG ( str_query ):
	try:
		cur_PG.execute(str_query) 
#		print "Wykonane zapytanie "+str_query
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
#		print "Wykonane zapytanie "+str_query
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
			#	print old_name_path 
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
	#	print root
		rename = 0
		insert_miej = 0
		for body in root.findall('body'):
			#print body
			for table in body.findall('table'):
				insert_osoba = 0
				LP_polozenie = 0
				th_tabelka = 'Brak'
				licz = table
				insert_wiel_udzl_bud_lok = 0
				for tr in table.findall('tr'):		
					for th in tr.findall('th'):
						th_tabelka = th.text.strip()
						#os.system("pause")
					
					print th_tabelka
					#os.system("pause")
					
					if 'Rubryka 0.1' in th_tabelka:
						for td in tr.findall('td'):
							print td.text
							ATRYB_TD = td.attrib
							TD_ID = ATRYB_TD.get('id')
							#Numer ksiegi	
							if 'Numer ksiegi' in td.text:
								licznik = 1
								for td_local in tr:
									if licznik == 3:
										nr_ksw = td_local.text.strip()
										print 'Nr KSW : '+ nr_ksw
									licznik = licznik+1	

					
					if 'Rubryka 2.3' in th_tabelka:
						
						for td in tr.findall('td'):
							if '1. Numer udzia' in td.text:
								licznik = 1
								for td_local in tr:
									if licznik == 4:
										nr_udzialu_bud = td_local.text.strip()
										print 'Numer udzialu bud:'+ nr_udzialu_bud
										insert_wiel_udzl_bud_lok =insert_wiel_udzl_bud_lok+1
									licznik = licznik+1		

							if '3. Wielko' in td.text:
								licznik = 1
								for td_local in tr:
									if licznik == 4:
										wiel_udzialu_bud = td_local.text.strip()
										insert_wiel_udzl_bud_lok =insert_wiel_udzl_bud_lok+10
										print 'wielkosc udzialu bud:'+ wiel_udzialu_bud
									licznik = licznik+1		
									
							if '4. Numer ksi' in td.text:
								licznik = 1
								for td_local in tr:
									if licznik == 3:
										nr_kw_bud = td_local.text.strip()
										insert_wiel_udzl_bud_lok =insert_wiel_udzl_bud_lok+100
										print 'Numer ksigi:'+ nr_kw_bud
									licznik = licznik+1		
							
							if '5. Numer lokalu' in td.text:
								licznik = 1
								for td_local in tr:
									if licznik == 3:
										nr_lok_bud = td_local.text.strip()
										insert_wiel_udzl_bud_lok =insert_wiel_udzl_bud_lok+1000
										print 'Numer lokalu:'+ nr_lok_bud

									licznik = licznik+1		
							print 'insert_wiel_udzl_bud_lok '+str(insert_wiel_udzl_bud_lok)
							if insert_wiel_udzl_bud_lok ==1111:
								insert_query = "insert into udz_lok_bud_Pow_KR_7(NR_KW,nr_udzialu,wiel_udzialu_bud,nr_kw_bud,nr_lok_bud) VALUES ('"+nr_ksw+"','"+nr_udzialu+"','"+wiel_udzialu_bud+"','"+nr_kw_bud+"','"+nr_lok_bud+"')"  
								insert_wiel_udzl_bud_lok = 0
								try: 
									exec_query_commit_PG(insert_query)
								except:
										print "ERR - " +insert_query
										os.system("pause")
								

					if 'Rubryka 1.3 - Po' in th_tabelka:
						for td in tr.findall('td'):
							ATRYB_TD = td.attrib
							TD_ID = ATRYB_TD.get('id')
							TD_ROWSPAN = ATRYB_TD.get('rowspan')
							print "insert_osoba - " + str(insert_osoba)
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
							
							#ta jakos peta 
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
								insert_query = "insert into polozenie_Pow_KR_7(NR_KW,LP_polozenie,WojewodztwoNazwa,PowiatNazwa,GminaNazwa,nazwa_miej,nr_poz) VALUES ('"+nr_ksw+"','"+LP_polozenie+"','"+WojewodztwoNazwa+"','"+PowiatNazwa+"','"+GminaNazwa+"','"+miej_nazwa+"',"+ nr_poz_miej +"::int)"  
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
					
						#---- obszar 
						#Obszar 
						if '1. Obszar' in td.text:
							licznik = 1
							for td_local in tr:
								if licznik == 3:
									obszar = td_local.text.strip()
									print 'Obszar : '+ obszar
									insert_query = "insert into pow_sum_dzial_Pow_KR_7(NR_KW, Pow_sum_dzialek) VALUES ('"+nr_ksw +"','"+obszar+"')"
									exec_query_commit_PG(insert_query)						

								licznik = licznik+1		
								
						
						#---- wielkosc udzialu 
						#nr udzialu 
						if '1. Numer udzia' in td.text:
							licznik = 1
							for td_local in tr:
								if licznik == 4:
									nr_udzialu = td_local.text.strip()
									print 'Numer udzialu: '+ nr_udzialu
								licznik = licznik+1		

						#wielkosc udzialu 
						if 'u (licznik/mianownik)' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									wielkosc_udzialu = td_local.text.strip()
#									if wielkosc_udzialu == '1/1':
#										nr_udzialu = '1'
									print 'wielkosc_udzialu: '+ wielkosc_udzialu
									insert_query = "insert into udzialy_Pow_KR_7(NR_KW, NR_udzialu,Udzial) VALUES ('"+nr_ksw +"','"+nr_udzialu+"','"+wielkosc_udzialu+"')"
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
									print '8'
									print td1.attrib
									if licznik == 3:
										nr_udzialu_oso = td1.text.strip()
										print 'Numer nr_udzialu_oso: '+ nr_udzialu_oso
									licznik = licznik+1	
						
						
						#pierwsze imie 
						if '2. Imi' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									pierw_imie = td_local.text.strip()
									print 'Pierwsze imie: '+ pierw_imie
									insert_osoba = insert_osoba+1
									print insert_osoba
								licznik = licznik+1
							if licznik < 4:
								insert_osoba = 0
						
						#drugie imie 
						if '3. Imi' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									drugie_imie = td_local.text.strip()
									print 'Drugie imie: '+ drugie_imie
									insert_osoba = insert_osoba+10
									print insert_osoba
								licznik = licznik+1	
							if licznik < 4:
								insert_osoba = 0								
								
						#nazwisko pierwszy czlon 
						if '4. Nazwisko / pierwszy' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									nazwisko_pierw_cz= td_local.text.strip()
									print 'Nazwisko 1: '+ nazwisko_pierw_cz
									insert_osoba = insert_osoba+100
									print insert_osoba
								licznik = licznik+1		
							if licznik < 4:
								insert_osoba = 0								

						#nazwisko pierwszy czlon 
						if '5. Drugi cz' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									nazwisko_drugi_cz = td_local.text.strip()
									print 'Nazwisko 2: '+ nazwisko_drugi_cz
									insert_osoba = insert_osoba+1000
									print insert_osoba
								licznik = licznik+1		
							if licznik < 4:
								insert_osoba = 0								
						#Imie ojca 
						if '6. Imi' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									imie_ojca = td_local.text.strip()
									print 'Imie ojca :'+ imie_ojca
									insert_osoba = insert_osoba+10000
									print insert_osoba
								licznik = licznik+1
							if licznik < 4:
								insert_osoba = 0						
						#Imie matki
						if '7. Imi' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									imie_matki = td_local.text.strip()
									print 'Imie matki:'+ imie_matki
									print insert_osoba 
									#os.system("pause")
									if insert_osoba==11111:
										insert_query = "insert into wlasciciele_Pow_KR_7(NR_KW, NR_udzialu,Imie,Drugie_imie,Nazwisko_1,Nazwisko_2,Ojciec,Marka) VALUES ('"+nr_ksw +"','"+nr_udzialu_oso+"','"+pierw_imie+"','"+drugie_imie+"','"+nazwisko_pierw_cz+"','"+nazwisko_drugi_cz+"','"+imie_ojca+"','"+imie_matki+"')"
										exec_query_commit_PG(insert_query)						
										insert_osoba = 0
								licznik = licznik+1				
						#instytucja 
						if '2. Nazwa' in td.text:
							licznik = 1
							for td_local in tr:								
								if licznik == 3:
									instytucja = td_local.text.strip()
									print 'Instytucja: '+ instytucja
									insert_query = "insert into wlasciciele_Pow_KR_7(NR_KW, NR_udzialu,Imie,Drugie_imie,Nazwisko_1,Nazwisko_2,Ojciec,Marka) VALUES ('"+nr_ksw +"','"+nr_udzialu_oso+"','"+instytucja+"','','','','','')"
									exec_query_commit_PG(insert_query)						
								licznik = licznik+1
						
						#os.system("pause")						
				ATRYB_TABLE = table.attrib
				TABLE_ID = ATRYB_TABLE.get('id')
				
				#Dzialki 
				if TABLE_ID == 'Dzialki': 
					dzialka_insert = 0
					for tbody in table.findall('tbody'):
						ATRYB_TBODY = tbody.attrib
						TBODY_ID = ATRYB_TBODY.get('id')
						if TBODY_ID == "DzialkaKW":
			#				print '\t\t tbody ----'+str(tbody)	
							licz_tr = 1
							for tr in tbody.findall('tr'):
								#print tr
								for td in tr.findall('td'):
									ATRYB_TD = td.attrib
									TD_ID = ATRYB_TD.get('id')
									
									#Numer Dzialki				
									if TD_ID == 'NumerDzialki':
										nr_dzialki = td.text.strip()
										if nr_dzialki =='':
											nr_dzialki ='PUSTE'
										print '\tNR dzialki: '+ nr_dzialki
										dzialka_insert = dzialka_insert+1
									#Numer Obrebu Ewid
									if TD_ID == 'NumerObrebuEwid':
										nr_obr_ewid = td.text.strip()
										if nr_obr_ewid =='':
											nr_obr_ewid ='PUSTE'
										print '\tNR Obrebu Ewid: '+ nr_obr_ewid
										dzialka_insert = dzialka_insert+10
									
									#Nazwa Obrebu Ewid
									if TD_ID == 'NazwaObrebuEwid':
										NazwaObrebuEwid = td.text.strip()
										if NazwaObrebuEwid =='':
											NazwaObrebuEwid ='PUSTE'
										print '\tNR Obrebu Ewid: '+ NazwaObrebuEwid
										dzialka_insert = dzialka_insert+100									
									
									#PolozenieDzialki
									for table_pol in td.findall('table'):
										print '1a'
										for tr_pol in table_pol.findall('tr'):
											print '2a'
											for td_pol in tr_pol.findall('td'):
												ATRYB_TD_POL = td_pol.attrib
												TD_ID_POL = ATRYB_TD_POL.get('id')
												print 'TD_ID_POL ' + str(TD_ID_POL)
												if TD_ID_POL == 'PolozenieDzialki':
													pol_dzialki = td_pol.text.strip()
													if pol_dzialki =='':
														pol_dzialki ='PUSTE'
													print '\tpol_dzialki: '+ pol_dzialki
													dzialka_insert = dzialka_insert+1000
												
												
						
									#Powierzchnia  Dzialki				
									if licz_tr == 11:
										licz_td = 1
										for td in tr.findall('td'):
											if licz_td == 3:
												pow_dzilki = td.text.strip()
												licz_td = 1
												#print dzialka_insert
												#os.system("pause")
												if dzialka_insert >= 1111:
												##	insert_query = "insert into dzialki_Pow_KR_7(NR_KW, NR_dzialki,Pow_dzialki, Pow_suma_dz) VALUES ('"+nr_ksw +"','"+nr_dzialki+"','"+pow_dzilki+"','"+obszar+"')"
													insert_query = "insert into dzialki_Pow_KR_7(NR_KW, NR_dzialki,NumerObrebuEwid,NazwaObrebuEwid,Pow_dzialki,pol_dzialki,plik) VALUES ('"+nr_ksw +"','"+nr_dzialki+"','"+nr_obr_ewid+"','"+NazwaObrebuEwid+"','"+pow_dzilki+"','"+pol_dzialki+"','"+xml_file_name+"')"  
													exec_query_commit_PG(insert_query)						
													dzialka_insert = 0
											licz_td = licz_td+1
										licz_tr = 1	
										
								licz_tr = licz_tr+1		
					
				


				#BUDYNKI  
				"""ident_bud varchar, -- IdentyfikatorBudynku  
				ident_dzi varchar, -- IdentyfikatorDzialki
				polozenie varchar, -- Polozenie
				ulica_plac varchar, Ulica
				nr_porz_bud varchar,  NrPorzadkowy
				przeznacz_bud varchar,PrzeznaczenieBudynku
				info_o_lok varchar, 14. Informacja o wy
				"""
				if TABLE_ID == 'Budynki': 
					
					for tbody in table.findall('tbody'):
						ATRYB_TBODY = tbody.attrib
						TBODY_ID = ATRYB_TBODY.get('id')
						if TBODY_ID == "BudynekKW":
							IdentyfikatorBudynku_par01 = ''
							IdentyfikatorBudynku_par02 = ''
							ident_dzialki = ''
							Polozenie = ''
							ulica = ''
							NrPorzadkowy = ''
							PrzeznaczenieBudynku ='test'
							info_o_local = ''
							for t_tr in tbody.findall('tr'):
								for t_td in t_tr.findall('td'):
									ATRYB_TD = t_td.attrib
									TD_ID = ATRYB_TD.get('id')
									
									#IdentyfikatorBudynku
									if '1. Identyfikator budynku' in t_td.text:
										for tt_td in t_tr:								
											ATRYB_tt_td = tt_td.attrib
											tt_td_ID = ATRYB_tt_td.get('id')
											print tt_td_ID
											
											#IdentyfikatorBudynku
											if tt_td_ID == 'IdentyfikatorBudynku':
												for DIV in tt_td.findall('div'):
													ATRYB_DIV = DIV.attrib
													DIV_ID = ATRYB_DIV.get('class')
													if DIV_ID == 'blad':
														IdentyfikatorBudynku_par01 = DIV.text.strip()
														if IdentyfikatorBudynku_par01 =='':
															IdentyfikatorBudynku_par01 ='PUSTE'
														print '\tNR IdentyfikatorBudynku_par01: '+ IdentyfikatorBudynku_par01
														
													
													
													if DIV_ID == 'poprawione':
														IdentyfikatorBudynku_par02 = DIV.text.strip()
														if IdentyfikatorBudynku_par02 =='':
															IdentyfikatorBudynku_par02 ='PUSTE'
														print '\tNR IdentyfikatorBudynku_par02: '+ IdentyfikatorBudynku_par02
															
											

									#PrzeznaczenieBudynku
									if TD_ID == 'PrzeznaczenieBudynku':
										PrzeznaczenieBudynku = t_td.text.strip()
										if PrzeznaczenieBudynku =='':
											PrzeznaczenieBudynku ='PUSTE'
										print '\tNR Identyfikator Budynku: '+ PrzeznaczenieBudynku
										
									
									
									if '2. Identyfikator dzia' in t_td.text:
										licznik = 1
										for td_local in t_tr:								
											if licznik == 2:
												if td_local.text.strip() == '- - -':
													ident_dzialki = '- - -'
													
												for t_table in td_local.findall('table'):
													for tt_tr in t_table.findall('tr'):
														for tt_td in tt_tr.findall('td'):
															ATRYB_tt_td = tt_td.attrib
															tt_td_ID = ATRYB_tt_td.get('id')
															#IdentyfikatorDzialki
															if tt_td_ID == 'IdentyfikatorDzialki':
																ident_dzialki = tt_td.text.strip()
																if ident_dzialki =='':
																	ident_dzialki ='PUSTE'
																print '\tNR Identyfikator Dzialki: '+ ident_dzialki
																
														
											licznik = licznik+1
									
									
									
									#Ulica Nr adresowy
									if '4. Dane adresowe' in t_td.text:
										Ulica = ''
										NrPorzadkowy = ''
										licznik = 1
										for td_local in t_tr:
											if licznik == 3:
												if td_local.text.strip() == '- - -':
													Ulica = '- - -'
											ATRYB_td_local = td_local.attrib
											td_local_ID = ATRYB_td_local.get('id')
											print td_local.text.strip()
											print td_local_ID
												
											#Ulica
											if td_local_ID == 'Ulica':
												Ulica = td_local.text.strip()
												if Ulica =='':
													Ulica ='PUSTE'
												print '\tUlica: '+ Ulica
																

									if TD_ID == 'NrPorzadkowy':
										NrPorzadkowy = t_td.text.strip()
										if NrPorzadkowy =='':
											NrPorzadkowy ='PUSTE'
										print '\tNr Porzadkowy: '+ NrPorzadkowy

									
							
									
									#NUMER I KW LOKALU
									if '14. Informacja o wyod' in t_td.text:
										LokaleWyodrebnioneNr = ''
										LokaleWyodrebnioneKw = ''
										licznik = 1
										for td_local in t_tr:								
											print td_local.text
											if licznik == 2:
												if td_local.text.strip() == '- - -':
													LokaleWyodrebnioneNr = '- - -'
													LokaleWyodrebnioneKw = '- - -'
													
												print td_local.text.strip()
																								
												for t_table in td_local.findall('table'):
													for tt_tr in t_table.findall('tr'):
														for tt_td in tt_tr.findall('td'):
															ATRYB_tt_td = tt_td.attrib
															tt_td_ID = ATRYB_tt_td.get('id')
															#LokaleWyodrebnioneNr
															if tt_td_ID == 'LokaleWyodrebnioneNr':
																LokaleWyodrebnioneNr = LokaleWyodrebnioneNr+'|'+tt_td.text.strip()
																if LokaleWyodrebnioneNr =='':
																	LokaleWyodrebnioneNr =LokaleWyodrebnioneNr+'|'+'PUSTE'
																print '\tNR lokalu: '+ LokaleWyodrebnioneNr
																
															
															if tt_td_ID == 'LokaleWyodrebnioneKw':
																LokaleWyodrebnioneKw = LokaleWyodrebnioneKw+'|'+tt_td.text.strip()
																if LokaleWyodrebnioneKw =='':
																	LokaleWyodrebnioneKw =LokaleWyodrebnioneNr+'|'+'PUSTE'
																print '\tKW Lokalu: '+ LokaleWyodrebnioneKw
																
														
											licznik = licznik+1
									
							
							
							insert_query = "insert into budynki_Pow_KR_7 (NR_KW, IdentyfikatorBudynku_par01,IdentyfikatorBudynku_par02,przeznaczeniebudynku, ident_dzi,LokaleWyodrebnioneNr,	LokaleWyodrebnioneKw,Ulica,NrPorzadkowy) VALUES ('"+nr_ksw +"','"+IdentyfikatorBudynku_par01+"','"+IdentyfikatorBudynku_par02+"','"+PrzeznaczenieBudynku+"','"+ident_dzialki+"','"+LokaleWyodrebnioneNr+"','"+LokaleWyodrebnioneKw+"','"+Ulica+"','"+NrPorzadkowy+"');"
							print insert_query
							exec_query_commit_PG(insert_query)
							
def creae_tables ():
	# dziala z numerem i powierzchnia 
	drop_quer = "drop table if exists dzialki_Pow_KR_7;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table dzialki_Pow_KR_7 (
	 NR_KW varchar,  
	 NR_dzialki varchar,
	 NumerObrebuEwid varchar,
	 NazwaObrebuEwid varchar,
	 Pow_dzialki varchar,
	 pol_dzialki varchar,
	 DzialkaUlica varchar,
	 plik varchar 
	);"""
	exec_query_commit_PG(create_query)
	
	# Powierzchnia sumy dzialek 
	drop_quer = "drop table if exists pow_sum_dzial_Pow_KR_7;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table pow_sum_dzial_Pow_KR_7 (
	 NR_KW varchar,  
	 Pow_sum_dzialek varchar
	);"""
	exec_query_commit_PG(create_query)
	
	#Wlasciciel z rodzice 
	drop_quer = "drop table if exists wlasciciele_Pow_KR_7;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table wlasciciele_Pow_KR_7 (
	 NR_KW varchar, 
	 NR_udzialu varchar, 	
	 Imie varchar,
	 Drugie_imie varchar,
	 Nazwisko_1 varchar,
	 Nazwisko_2 varchar,
	 Ojciec varchar,
	 Marka varchar
	);"""
	exec_query_commit_PG(create_query)

	#udzialy 
	drop_quer = "drop table if exists udzialy_Pow_KR_7;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table udzialy_Pow_KR_7 (
	 NR_KW varchar, 
	 NR_udzialu varchar, 	
	 Udzial varchar
	);"""
	exec_query_commit_PG(create_query)

	#polozenie
	drop_quer = "drop table if exists polozenie_Pow_KR_7;"
	exec_query_commit_PG(drop_quer)

#NR_KW,WojewodztwoNazwa,PowiatNazwa,GminaNazwa,nazwa_miej,nr_poz
	create_query = """create table polozenie_Pow_KR_7 (
	 NR_KW varchar,
	 LP_polozenie varchar,
	 WojewodztwoNazwa varchar,
	 PowiatNazwa varchar,
	 GminaNazwa varchar,
	 nazwa_miej varchar,
	 nr_poz int
	);"""
	exec_query_commit_PG(create_query)
	
# budynki
	drop_quer = "drop table if exists budynki_Pow_KR_7;"
	exec_query_commit_PG(drop_quer)

	create_query = """create table budynki_Pow_KR_7 (
	NR_KW varchar,
	IdentyfikatorBudynku_par01 varchar,
	IdentyfikatorBudynku_par02 varchar,
	przeznaczeniebudynku varchar,
	ident_dzi varchar,
	LokaleWyodrebnioneNr varchar,
	LokaleWyodrebnioneKw varchar,
	Ulica varchar,
	NrPorzadkowy varchar
	);"""
	exec_query_commit_PG(create_query)
#udzialy lokalowe 
	drop_quer = "drop table if exists udz_lok_bud_Pow_KR_7;"
	exec_query_commit_PG(drop_quer)
	create_query = """create table udz_lok_bud_Pow_KR_7(
	NR_KW varchar,
	nr_udzialu varchar,
	wiel_udzialu_bud varchar,
	nr_kw_bud varchar, 
	nr_lok_bud varchar
	);"""
	exec_query_commit_PG(create_query)	
#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
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
	
main_dir_list = ['f:\\TMCE\\Ania\\pow_krakowski\\czytanie_kw\\KRAKOWSKI\\7_KW\\2_Pobrane EKW\\budynki\\KW\\']

#polacznie z baza POSTGRES 
teraz = time.asctime( time.localtime(time.time()))
naz_log.write('START -' +teraz + '\n')
try:
	conn_PG = psycopg2.connect("dbname='krakowski_7kw' user='postgres' host='127.0.0.1' password='aaaaaa'")
	naz_log.write(teraz + ' [INF] Polaczono z baza danych\n')
except:
	print "I am unable to connect to the database"
	err_log.write(teraz +' - [ERR] Nie udalo sie naiazac polacznia z baz\n')

cur_PG = conn_PG.cursor()
E = lxml.builder.ElementMaker()
creae_tables()
for main_dir in main_dir_list:
	get_files_txt()
os.system("pause")