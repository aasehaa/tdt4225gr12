""" 
Insert data psudeocode
* Bruk os.walk
* For hver user:
    if user_dirname in labeled_ids.txt: has_labels = True
    sett inn bruker i DB
    for hver .plt
        activityID = mysql_connector.GetAutoIncrementThing()
        if antall lines > 2506: skip (husk å anta 2506 i rapporten)
        hent start [6] og slutt [-1] datetime
        Sett inn aktivitet i DB (med transportation_mode = '')
        if has_labels:
            sjekk at start in labels[start] og slutt in labels[slutt]
                hvis match: sett transportation_mode til labels[Mode]
        sett inn plt-data i TrackPoint DB
        # Piazza-spørsmål: kan vi anta at data ikke er feil?   
## Batches of data instead?      
"""