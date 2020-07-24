from datetime import datetime

import mysql.connector
from mysql.connector import Error

# Mysql connection to dabasase

db_kenya_emr = mysql.connector.connect(host="127.0.0.1",  # your host
                     user="user",  # username
                     passwd="password",  # password
                     db="database")  # name of the database

db_afya_ehms = mysql.connector.connect(host="your host ip",  # your host
                user="user",  # username
                passwd="password",  # password
                db="name_of_your_db")  # name of the database


try:
    print("script started")
    # Create a Cursor object to execute queries.
    cur_afya_ehms = db_afya_ehms.cursor()
    cur_kenya_emr = db_kenya_emr.cursor()

    #check last sysnch timestamp
    lastupdate=None

    with open('synch.properties', 'r') as file:
        data = file.read().replace('\n', '')
        try:
            prop=data.split("=")[0]
            if(prop=="lastupdate"):
                lastupdate=datetime.strptime(data.split("=")[1], '%Y-%m-%d %H:%M:%S')

        except Exception as e:
            print (e)

        try:
            max_lastupdate_timestamp_sql='''Select max(date_created) FROM person'''
            cur_kenya_emr.execute(max_lastupdate_timestamp_sql)
            for row in cur_kenya_emr.fetchone():
                max_lastupdate_timestamp=row

            records_to_replicate_sql = '''SELECT prsn.gender, prsn.birthdate, prsn.birthdate_estimated, prsn.dead, prsn.death_date, prsn.cause_of_death, 
                                               prsn.date_changed, prsn.voided,pname.given_name as gname,pname.middle_name mname,pname.family_name as fname,
                                               pt.date_changed,pt.date_created,pt.voided,pt.date_voided,pt.patient_id
                                               FROM person prsn inner join person_name pname on pname.person_id=prsn.person_id
                                               inner join patient pt on pt.patient_id=prsn.person_id
                                               where prsn.date_created > '%s' ''' %lastupdate
            cur_kenya_emr.execute(records_to_replicate_sql)

            for row in cur_kenya_emr.fetchall():
                print(" found matching rows === ", row)
                sanitized_row=None
                try:

                    sanitized_values = []
                    for r in range(len(row)):
                        if(row[r]==None):
                            if(r==3):
                                sanitized_values.append(0)
                            else:
                                sanitized_values.append('Null')
                        else:
                            sanitized_values.append(row[r])
                    sanitized_row = tuple(sanitized_values)

                    compare_query='''SELECT *
                                               FROM person prsn inner join person_name pname on pname.person_id=prsn.person_id
                                                where prsn.gender='%s' and prsn.birthdate='%s' and prsn.birthdate_estimated='%s' and prsn.dead='%s' and 
                                              prsn.voided='%s' and pname.given_name='%s' and pname.middle_name='%s' and pname.family_name='%s'
                                                   ''' % ( sanitized_row[0], sanitized_row[1], sanitized_row[2], sanitized_row[3], sanitized_row[7], sanitized_row[8], sanitized_row[9], sanitized_row[10]  )
                    cur_afay_ehms.execute(compare_query)

                    row1=cur_afay_ehms.fetchall()
                    print("Found matching records === %s " % len(row1))
                    if(len(row1)==0):
                        # update person record
                        update_person_string='''
                                           insert into person( gender, birthdate,birthdate_estimated, dead, death_date, cause_of_death,creator, changed_by, date_changed, voided,uuid, date_created)
                                           values('%s' ,'%s',%s,%s,NULL, %s ,%s ,%s ,NULL ,%s ,uuid(),'%s')
                                           ''' % ( sanitized_row[0], sanitized_row[1], sanitized_row[2], sanitized_row[3], sanitized_row[5], 1, 1, sanitized_row[7],sanitized_row[12] )

                  
                        cur_afay_ehms.execute(update_person_string)
                        db_afya_ehms.commit()

                        p_id=cur_afay_ehms.lastrowid

                        # update person_name record
                        update_person_name_string = '''
                                                               insert into person_name( middle_name, given_name,family_name, creator, person_id,voided_by,date_created,uuid)
                                                               values('%s' ,'%s','%s','%s','%s',Null,'%s', uuid())
                                                               ''' % ( sanitized_row[9], sanitized_row[8], sanitized_row[10], 1,p_id,sanitized_row[12])

                        cur_afay_ehms.execute(update_person_name_string)
                        db_afya_ehms.commit()

                        

                        #update patient record
                        update_patient_string = '''
                                                                                   insert into patient( patient_id, date_created,date_changed, voided, date_voided,creator)
                                                                                   values('%s' ,'%s',NULL,'%s',NULL,'1')
                                                                                   ''' % ( p_id,  sanitized_row[12], sanitized_row[13])



                        cur_afay_ehms.execute(update_patient_string)
                        db_afya_ehms.commit()



                        #update patient identifier
                        patient_identfier_type_id=None
                        patient_identfier_type_id_sql=''' select patient_identifier_type_id from patient_identifier_type where name="OpenMRS ID" '''

                        cur_afay_ehms.execute(patient_identfier_type_id_sql)

                        for row in cur_afay_ehms.fetchone():
                            patient_identfier_type_id=row

                        patient_identifier_record=None

                        patient_identifier_record_sql=''' SELECT pi.identifier,pi.identifier_type,pi.date_created,pi.date_changed,pi.uuid FROM patient_identifier pi INNER JOIN patient_identifier_type pit ON pit.patient_identifier_type_id=pi.identifier_type where pit.patient_identifier_type_id=(select patient_identifier_type_id from patient_identifier_type where uuid="8d793bee-c2cc-11de-8d13-0010c6dffd0f") AND pi.patient_id='%s'  '''%(sanitized_row[15])

                        print(patient_identifier_record_sql)


                        cur_kenya_emr.execute(patient_identifier_record_sql)

                        for rows in cur_kenya_emr:
                            patient_identifier_record=rows
                            print("123",patient_identifier_record)

                

                        update_patient_identifier=''' insert into  patient_identifier(patient_id, identifier, identifier_type,preferred,location_id, creator,date_created, voided, voided_by, date_voided, void_reason,uuid, date_changed, changed_by)  values('%s','%s',%s,1,1,1,'%s',0,NULL,NULL,NULL,uuid(),NULL,NULL) '''%(p_id, patient_identifier_record[0],patient_identfier_type_id,patient_identifier_record[2])

                        cur_afay_ehms.execute(update_patient_identifier)

                        db_afya_ehms.commit()

                    else:
                        print("Found records %s "%len(row1))


                except Exception as e:
                    print("compare query error")
                    print (e)
        except Exception as e:
            print("query compare error")
            print (e)
    #update with next last update timestamp
    print ("Next check start timestamp = ", max_lastupdate_timestamp)
    f = open("synch.properties", "w")
    f.write("lastupdate=%s" %max_lastupdate_timestamp)
    f.close()
except Exception as e:
    print (e)
# Select data from table using SQL query.

