"""Market calendar — trading holidays, shared by the server and the UI.

Single source of truth. The dashboard's JavaScript holiday table is
GENERATED from EXCHANGE_HOLIDAYS below (see dashboard.py), so the
open/closed badge and the server-side "should I even fetch this?"
decision can never disagree. Before this module the table lived only in
the JS, which meant the price refresh had no idea a market was shut and
happily re-fetched a closed exchange all day, writing flat 0.00% rows
that looked like a bug.

Entry format per country: "MM-DD:Name" (same date every year) or
"YYYY-MM-DD:Name" (movable — lunar/Islamic dates that need an
ephemeris and must be curated). Easter-derived Christian holidays are
computed for any year: a market observes one only if its curated list
already names it.
"""

from datetime import date as _date, timedelta as _timedelta

EXCHANGE_HOLIDAYS: dict[str, list[str]] = {
    'Argentina': ['01-01:New Year', '02-16:Carnival', '02-17:Carnival', '03-24:Memory Day', '04-02:Malvinas', '05-01:Labor Day', '05-25:May Revolution', '06-15:Güemes', '06-20:Flag Day', '07-09:Independence', '08-17:San Martín', '10-12:Diversity', '11-23:Sovereignty', '12-08:Immaculate Conception', '12-25:Christmas', '2026-04-02:Maundy Thursday', '2026-04-03:Good Friday'],
    'Australia': ['01-01:New Year', '01-26:Australia Day', '12-25:Christmas', '12-26:Boxing Day', '12-28:Boxing Day Obs', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-04-25:ANZAC Day', '2026-06-08:King Birthday'],
    'Austria': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '08-15:Assumption', '10-26:National Day', '11-01:All Saints', '12-08:Immaculate Conception', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension'],
    'Bangladesh': ['02-21:Language Movement', '03-17:Mujib Birthday', '03-26:Independence', '04-14:Bengali New Year', '05-01:May Day', '08-15:National Mourning', '12-16:Victory Day', '12-25:Christmas', '2026-03-22:Eid al-Fitr', '2026-04-13:Bengali New Year', '2026-05-31:Eid al-Adha', '2026-06-01:Eid al-Adha'],
    'Belgium': ['01-01:New Year', '05-01:Labor Day', '07-21:National Day', '08-15:Assumption', '11-01:All Saints', '11-11:Armistice', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension', '2026-05-25:Whit Monday'],
    'Brazil': ['01-01:New Year', '04-21:Tiradentes', '05-01:Labor Day', '06-19:Corpus Christi', '09-07:Independence', '10-12:Lady of Aparecida', '11-02:All Souls', '11-15:Republic Day', '11-20:Black Awareness', '12-24:Christmas Eve', '12-25:Christmas', '12-31:New Year Eve', '2026-02-16:Carnival', '2026-02-17:Carnival', '2026-04-03:Good Friday'],
    'Canada': ['01-01:New Year', '07-01:Canada Day', '12-25:Christmas', '12-26:Boxing Day', '2026-02-16:Family Day', '2026-04-03:Good Friday', '2026-05-18:Victoria Day', '2026-08-03:Civic Holiday', '2026-09-07:Labor Day', '2026-10-12:Thanksgiving'],
    'Chile': ['01-01:New Year', '05-01:Labor Day', '05-21:Naval Glories', '06-29:St Peter & St Paul', '07-16:Lady of Carmen', '08-15:Assumption', '09-18:Independence', '09-19:Army Day', '10-12:Discovery', '11-01:All Saints', '12-08:Immaculate Conception', '12-25:Christmas', '12-31:Bank Holiday', '2026-04-03:Good Friday'],
    'China (Shanghai)': ['01-01:New Year', '05-01:Labor Day', '2026-02-16:Lunar New Year', '2026-02-17:Lunar New Year', '2026-02-18:Lunar New Year', '2026-04-06:Qingming', '2026-05-04:Labor Day', '2026-06-22:Dragon Boat', '2026-09-25:Mid-Autumn', '2026-09-28:National Day', '2026-09-29:National Day', '2026-09-30:National Day', '2026-10-01:National Day', '2026-10-02:National Day', '2026-10-05:National Day', '2026-10-06:National Day', '2026-10-07:National Day', '2026-10-08:National Day'],
    'China (Shenzhen)': ['01-01:New Year', '05-01:Labor Day', '2026-02-16:Lunar New Year', '2026-02-17:Lunar New Year', '2026-02-18:Lunar New Year', '2026-04-06:Qingming', '2026-05-04:Labor Day', '2026-06-22:Dragon Boat', '2026-09-25:Mid-Autumn', '2026-09-28:National Day', '2026-09-29:National Day', '2026-09-30:National Day', '2026-10-01:National Day', '2026-10-02:National Day', '2026-10-05:National Day', '2026-10-06:National Day', '2026-10-07:National Day', '2026-10-08:National Day'],
    'Croatia': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '06-22:Antifascist Struggle', '08-05:Victory Day', '08-15:Assumption', '11-01:All Saints', '12-25:Christmas', '12-26:St Stephen', '2026-04-06:Easter Monday'],
    'Czech Republic': ['01-01:New Year', '05-01:Labor Day', '05-08:Liberation', '07-05:St Cyril & Methodius', '07-06:Jan Hus', '09-28:St Wenceslaus', '10-28:Independence', '11-17:Freedom & Democracy', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '2026-04-03:Good Friday', '2026-04-06:Easter Monday'],
    'Denmark': ['01-01:New Year', '05-01:Labor Day', '06-05:Constitution Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-02:Maundy Thursday', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-01:General Prayer', '2026-05-14:Ascension', '2026-05-25:Whit Monday'],
    'Egypt': ['01-07:Coptic Christmas', '01-25:Revolution Day', '04-25:Sinai Liberation', '05-01:Labor Day', '07-23:Revolution Day', '10-06:Armed Forces', '2026-04-13:Sham El-Nessim', '2026-04-19:Eid al-Fitr', '2026-04-20:Eid al-Fitr', '2026-03-22:Eid al-Fitr', '2026-03-23:Eid al-Fitr', '2026-05-31:Eid al-Adha'],
    'Finland': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '12-06:Independence', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension', '2026-06-19:Midsummer Eve'],
    'France': ['01-01:New Year', '05-01:Labor Day', '05-08:Victory Day', '07-14:Bastille Day', '08-15:Assumption', '11-01:All Saints', '11-11:Armistice', '12-25:Christmas', '12-26:St Stephen', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension', '2026-05-25:Whit Monday'],
    'Germany': ['01-01:New Year', '05-01:Labor Day', '10-03:Unity Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension', '2026-05-25:Whit Monday'],
    'Greece': ['01-01:New Year', '01-06:Epiphany', '03-25:Independence', '05-01:Labor Day', '08-15:Assumption', '10-28:Ohi Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-02-23:Clean Monday', '2026-04-10:Orthodox Good Friday', '2026-04-13:Orthodox Easter Monday'],
    'Hong Kong': ['01-01:New Year', '05-01:Labor Day', '07-01:HKSAR Day', '10-01:National Day', '12-25:Christmas', '12-26:Boxing Day', '2026-02-17:Lunar New Year', '2026-02-18:Lunar New Year', '2026-02-19:Lunar New Year', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-04-07:Ching Ming', '2026-05-25:Buddha Birthday', '2026-06-19:Dragon Boat', '2026-09-26:Mid-Autumn'],
    'Hungary': ['01-01:New Year', '03-15:Revolution', '05-01:Labor Day', '08-20:St Stephen', '10-23:Republic Day', '11-01:All Saints', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-25:Whit Monday'],
    'Iceland': ['01-01:New Year', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-02:Maundy Thursday', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-04-23:First Day of Summer', '2026-05-01:Labor Day', '2026-05-14:Ascension', '2026-05-25:Whit Monday'],
    'India': ['01-26:Republic Day', '03-31:Eid al-Fitr', '05-01:May Day', '08-15:Independence', '10-02:Gandhi Jayanti', '12-25:Christmas', '2026-03-04:Holi', '2026-04-03:Good Friday', '2026-04-14:Ambedkar Jayanti', '2026-04-21:Mahavir Jayanti', '2026-09-23:Eid al-Adha', '2026-10-21:Diwali', '2026-11-04:Diwali Padwa'],
    'Indonesia': ['01-01:New Year', '05-01:Labor Day', '06-01:Pancasila Day', '08-17:Independence', '12-25:Christmas', '2026-02-17:Lunar New Year', '2026-03-22:Eid al-Fitr', '2026-03-23:Eid al-Fitr', '2026-04-03:Good Friday', '2026-05-14:Ascension', '2026-06-01:Vesak', '2026-06-01:Eid al-Adha', '2026-09-25:Prophet Birthday'],
    'Iraq': ['01-01:New Year', '01-06:Army Day', '05-01:Labor Day', '07-14:Republic Day', '10-03:National Day', '2026-03-22:Eid al-Fitr', '2026-05-31:Eid al-Adha', '2026-06-01:Eid al-Adha'],
    'Ireland': ['01-01:New Year', '03-17:St Patrick', '05-01:May Bank', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday'],
    'Israel': ['2026-04-02:Passover Eve', '2026-04-03:Passover', '2026-04-08:Passover', '2026-04-09:Passover', '2026-04-22:Independence Day', '2026-05-22:Shavuot', '2026-09-12:Rosh Hashanah', '2026-09-13:Rosh Hashanah', '2026-09-21:Yom Kippur', '2026-09-22:Yom Kippur', '2026-09-26:Sukkot', '2026-10-03:Simchat Torah'],
    'Italy': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '06-02:Republic Day', '08-15:Assumption', '12-08:Immaculate Conception', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday'],
    'Japan': ['01-01:New Year', '01-02:New Year', '01-03:New Year', '02-11:National Foundation', '02-23:Emperor Birthday', '04-29:Showa Day', '05-03:Constitution Day', '05-04:Greenery Day', '05-05:Children Day', '08-11:Mountain Day', '11-03:Culture Day', '11-23:Labor Thanksgiving', '12-31:New Year Eve', '2026-01-12:Coming of Age', '2026-03-21:Vernal Equinox', '2026-05-06:Children Day Obs', '2026-07-20:Marine Day', '2026-09-21:Respect for the Aged', '2026-09-22:Autumnal Equinox', '2026-10-12:Sports Day'],
    'Kenya': ['01-01:New Year', '05-01:Labor Day', '06-01:Madaraka Day', '10-10:Huduma Day', '10-20:Mashujaa', '12-12:Jamhuri Day', '12-25:Christmas', '12-26:Boxing Day', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-03-21:Eid al-Fitr'],
    'Lithuania': ['01-01:New Year', '02-16:Restoration of State', '03-11:Restoration of Independence', '05-01:Labor Day', '06-24:Midsummer', '07-06:Statehood', '08-15:Assumption', '11-01:All Saints', '11-02:All Souls', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '2026-04-05:Easter', '2026-04-06:Easter Monday', '2026-05-03:Mother Day'],
    'Malaysia': ['01-01:New Year', '02-01:Federal Territory Day', '05-01:Labor Day', '06-02:Agong Birthday', '08-31:National Day', '09-16:Malaysia Day', '12-25:Christmas', '2026-02-16:Lunar New Year', '2026-02-17:Lunar New Year', '2026-03-21:Eid al-Fitr', '2026-03-31:Hari Raya', '2026-06-01:Wesak', '2026-08-29:Maulidur Rasul', '2026-11-09:Deepavali'],
    'Mexico': ['01-01:New Year', '02-02:Constitution Day', '03-16:Benito Juarez', '05-01:Labor Day', '09-16:Independence', '11-02:Day of the Dead', '11-16:Revolution Day', '12-12:Lady of Guadalupe', '12-25:Christmas', '2026-04-02:Maundy Thursday', '2026-04-03:Good Friday'],
    'Netherlands': ['01-01:New Year', '05-01:Labor Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-04-27:King Day', '2026-05-14:Ascension', '2026-05-25:Whit Monday'],
    'New Zealand': ['01-01:New Year', '01-02:Day after New Year', '02-06:Waitangi Day', '04-25:ANZAC Day', '12-25:Christmas', '12-26:Boxing Day', '12-28:Boxing Day Obs', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-06-01:King Birthday', '2026-10-26:Labor Day'],
    'Nigeria': ['01-01:New Year', '05-01:Workers Day', '05-29:Democracy Day', '06-12:Democracy Day', '10-01:Independence', '12-25:Christmas', '12-26:Boxing Day', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-03-21:Eid al-Fitr', '2026-05-31:Eid al-Adha'],
    'Norway': ['01-01:New Year', '05-01:Labor Day', '05-17:Constitution Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-02:Maundy Thursday', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension', '2026-05-25:Whit Monday'],
    'Pakistan': ['02-05:Kashmir Day', '03-23:Pakistan Day', '05-01:Labor Day', '08-14:Independence', '11-09:Iqbal Day', '12-25:Quaid-e-Azam', '2026-03-22:Eid al-Fitr', '2026-05-31:Eid al-Adha', '2026-06-01:Eid al-Adha', '2026-06-02:Eid al-Adha'],
    'Philippines': ['01-01:New Year', '02-25:EDSA Revolution', '04-09:Day of Valor', '05-01:Labor Day', '06-12:Independence', '08-21:Ninoy Aquino', '08-31:National Heroes', '11-01:All Saints', '11-30:Bonifacio Day', '12-08:Immaculate Conception', '12-25:Christmas', '12-30:Rizal Day', '12-31:New Year Eve', '2026-04-02:Maundy Thursday', '2026-04-03:Good Friday'],
    'Poland': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '05-03:Constitution Day', '08-15:Assumption', '11-01:All Saints', '11-11:Independence', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '2026-04-06:Easter Monday'],
    'Portugal': ['01-01:New Year', '04-25:Freedom Day', '05-01:Labor Day', '06-10:Portugal Day', '08-15:Assumption', '10-05:Republic Day', '11-01:All Saints', '12-01:Restoration', '12-08:Immaculate Conception', '12-24:Christmas Eve', '12-25:Christmas', '12-31:New Year Eve', '2026-04-03:Good Friday'],
    'Qatar': ['12-18:National Day', '2026-03-22:Eid al-Fitr', '2026-03-23:Eid al-Fitr', '2026-03-24:Eid al-Fitr', '2026-05-31:Eid al-Adha', '2026-06-01:Eid al-Adha', '2026-06-02:Eid al-Adha', '2026-06-03:Eid al-Adha'],
    'Romania': ['01-01:New Year', '01-02:New Year', '01-24:Union Day', '05-01:Labor Day', '06-01:Children Day', '08-15:Assumption', '11-30:St Andrew', '12-01:National Day', '12-25:Christmas', '12-26:St Stephen', '2026-04-10:Orthodox Good Friday', '2026-04-13:Orthodox Easter Monday', '2026-06-01:Whit Monday'],
    'Saudi Arabia': ['09-23:National Day', '2026-03-21:Eid al-Fitr', '2026-03-22:Eid al-Fitr', '2026-03-23:Eid al-Fitr', '2026-03-24:Eid al-Fitr', '2026-05-31:Eid al-Adha', '2026-06-01:Eid al-Adha', '2026-06-02:Eid al-Adha', '2026-06-03:Eid al-Adha'],
    'Serbia': ['01-01:New Year', '01-02:New Year', '01-07:Orthodox Christmas', '02-15:Statehood', '02-16:Statehood', '05-01:Labor Day', '05-02:Labor Day', '11-11:Armistice', '2026-04-10:Orthodox Good Friday', '2026-04-13:Orthodox Easter Monday'],
    'Singapore': ['01-01:New Year', '05-01:Labor Day', '08-09:National Day', '12-25:Christmas', '2026-02-16:Lunar New Year', '2026-02-17:Lunar New Year', '2026-04-03:Good Friday', '2026-05-01:Labor Day', '2026-06-01:Vesak', '2026-08-09:National Day', '2026-09-25:Hari Raya Haji', '2026-11-08:Deepavali'],
    'Slovakia': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '05-08:Liberation', '07-05:St Cyril & Methodius', '08-29:Uprising', '09-01:Constitution', '09-15:Lady of Sorrows', '11-01:All Saints', '11-17:Freedom Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '2026-04-03:Good Friday', '2026-04-06:Easter Monday'],
    'Slovenia': ['01-01:New Year', '01-02:New Year', '02-08:Culture Day', '04-27:Resistance', '05-01:Labor Day', '05-02:Labor Day', '06-25:Statehood', '08-15:Assumption', '10-31:Reformation', '11-01:All Saints', '12-25:Christmas', '12-26:Independence', '2026-04-06:Easter Monday'],
    'South Africa': ['01-01:New Year', '03-21:Human Rights', '04-27:Freedom Day', '05-01:Workers Day', '06-16:Youth Day', '08-09:Womens Day', '09-24:Heritage Day', '12-16:Reconciliation', '12-25:Christmas', '12-26:Day of Goodwill', '2026-04-03:Good Friday', '2026-04-06:Family Day'],
    'South Korea': ['01-01:New Year', '03-01:Independence', '05-05:Children Day', '06-06:Memorial Day', '08-15:Liberation', '10-03:National Foundation', '10-09:Hangul Day', '12-25:Christmas', '2026-02-16:Lunar New Year', '2026-02-17:Lunar New Year', '2026-02-18:Lunar New Year', '2026-05-01:Labor Day', '2026-05-25:Buddha Birthday', '2026-09-24:Chuseok', '2026-09-25:Chuseok'],
    'Spain': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '08-15:Assumption', '10-12:National Day', '11-01:All Saints', '12-06:Constitution', '12-08:Immaculate Conception', '12-24:Christmas Eve', '12-25:Christmas', '12-31:New Year Eve', '2026-04-02:Maundy Thursday', '2026-04-03:Good Friday'],
    'Sri Lanka': ['02-04:Independence', '05-01:Labor Day', '05-22:Vesak', '12-25:Christmas', '2026-04-13:Sinhala New Year', '2026-04-14:Sinhala New Year', '2026-05-23:Vesak', '2026-08-08:Esala Poya'],
    'Sweden': ['01-01:New Year', '01-06:Epiphany', '05-01:Labor Day', '06-06:National Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension'],
    'Switzerland': ['01-01:New Year', '01-02:Berchtold', '05-01:Labor Day', '08-01:National Day', '12-24:Christmas Eve', '12-25:Christmas', '12-26:St Stephen', '12-31:New Year Eve', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-14:Ascension'],
    'Taiwan': ['01-01:New Year', '02-28:Peace Memorial', '04-04:Tomb Sweeping', '12-25:Christmas', '2026-02-16:Lunar New Year', '2026-02-17:Lunar New Year', '2026-02-18:Lunar New Year', '2026-02-19:Lunar New Year', '2026-02-20:Lunar New Year', '2026-04-06:Tomb Sweeping', '2026-09-25:Mid-Autumn', '2026-10-09:National Day'],
    'Thailand': ['01-01:New Year', '01-02:New Year', '04-06:Chakri Memorial', '04-13:Songkran', '04-14:Songkran', '04-15:Songkran', '05-01:Labor Day', '05-04:Coronation', '07-28:King Birthday', '08-12:Mother Day', '10-13:King Bhumibol Memorial', '10-23:Chulalongkorn', '12-07:King Father Birthday', '12-10:Constitution Day', '12-25:Christmas', '12-31:New Year Eve', '2026-06-01:Visakha Bucha', '2026-07-29:Asarnha Bucha', '2026-07-30:Buddhist Lent'],
    'Turkey': ['01-01:New Year', '04-23:National Sovereignty', '05-01:Labor Day', '05-19:Atatürk Memorial', '07-15:Democracy Day', '08-30:Victory Day', '10-29:Republic Day', '2026-03-21:Eid al-Fitr', '2026-03-22:Eid al-Fitr', '2026-03-23:Eid al-Fitr', '2026-05-31:Eid al-Adha', '2026-06-01:Eid al-Adha'],
    'UAE': ['01-01:New Year', '12-02:National Day', '12-03:National Day', '2026-03-22:Eid al-Fitr', '2026-03-23:Eid al-Fitr', '2026-03-24:Eid al-Fitr', '2026-05-31:Eid al-Adha', '2026-06-01:Eid al-Adha', '2026-06-02:Eid al-Adha', '2026-06-19:Hijri New Year', '2026-12-12:Prophet Birthday'],
    'UK': ['01-01:New Year', '05-01:May Bank', '12-25:Christmas', '12-26:Boxing Day', '2026-04-03:Good Friday', '2026-04-06:Easter Monday', '2026-05-04:Early May Bank', '2026-05-25:Spring Bank', '2026-08-31:Summer Bank'],
    'US': ['01-01:New Year', '06-19:Juneteenth', '07-04:Independence', '12-25:Christmas', '2026-01-19:MLK Day', '2026-02-16:Presidents Day', '2026-04-03:Good Friday', '2026-05-25:Memorial Day', '2026-09-07:Labor Day', '2026-11-26:Thanksgiving', '2026-11-27:Day after Thanksgiving'],
    'Vietnam': ['01-01:New Year', '04-30:Reunification', '05-01:Labor Day', '09-02:National Day', '2026-02-16:Lunar New Year', '2026-02-17:Lunar New Year', '2026-02-18:Lunar New Year', '2026-02-19:Lunar New Year', '2026-02-20:Lunar New Year', '2026-04-26:Hung Kings', '2026-04-29:Reunification Obs'],
}

# Exchange code → country key above. Only codes whose country actually
# has a holiday list; anything else resolves to None and is treated as
# "no holiday data" (we then assume the market is open, which is the
# safe direction — a needless fetch costs a second, a wrongly-skipped
# fetch leaves a stale price).
EXCHANGE_COUNTRY: dict[str, str] = {
    'ADX': 'UAE',
    'AMEX': 'US',
    'ASX': 'Australia',
    'ATHEX': 'Greece',
    'B3': 'Brazil',
    'BCBA': 'Argentina',
    'BELEX': 'Serbia',
    'BET': 'Hungary',
    'BIST': 'Turkey',
    'BIT': 'Italy',
    'BME': 'Spain',
    'BMV': 'Mexico',
    'BSSE': 'Slovakia',
    'BVB': 'Romania',
    'BVS': 'Chile',
    'CNSX': 'Canada',
    'CSE': 'Denmark',
    'CSEL': 'Sri Lanka',
    'CSE_CA': 'Canada',
    'DFM': 'UAE',
    'DSEB': 'Bangladesh',
    'EGX': 'Egypt',
    'EUR_BE': 'Belgium',
    'EUR_FR': 'France',
    'EUR_IE': 'Ireland',
    'EUR_NL': 'Netherlands',
    'EUR_PT': 'Portugal',
    'FRA': 'Germany',
    'HKSE': 'Hong Kong',
    'HOSE': 'Vietnam',
    'HSE': 'Finland',
    'ICE': 'Iceland',
    'ICEX': 'Iceland',
    'IDX': 'Indonesia',
    'IOB': 'UK',
    'ISX': 'Iraq',
    'JPX': 'Japan',
    'JSE': 'South Africa',
    'KLSE': 'Malaysia',
    'KRX': 'South Korea',
    'LIT': 'Lithuania',
    'LJSE': 'Slovenia',
    'LSE': 'UK',
    'NASDAQ': 'US',
    'NEO': 'Canada',
    'NGX': 'Nigeria',
    'NSEK': 'Kenya',
    'NYSE': 'US',
    'NZX': 'New Zealand',
    'OMX': 'Sweden',
    'OSE': 'Norway',
    'OTC': 'US',
    'PNK': 'US',
    'PSE': 'Philippines',
    'PSE_CZ': 'Czech Republic',
    'PSX': 'Pakistan',
    'QSE': 'Qatar',
    'SET': 'Thailand',
    'SGX': 'Singapore',
    'SSE': 'China (Shanghai)',
    'SWX': 'Switzerland',
    'SZSE': 'China (Shenzhen)',
    'TADAWUL': 'Saudi Arabia',
    'TASE': 'Israel',
    'TSX': 'Canada',
    'TSXV': 'Canada',
    'TWSE': 'Taiwan',
    'VAN': 'Canada',
    'VSE': 'Canada',
    'WBAG': 'Austria',
    'WSE': 'Poland',
    'ZSE': 'Croatia',
}


# Markets that shift a weekend holiday to the next business day
# ("day in lieu"). This is why SGX was shut on Mon 10 Aug 2026:
# Singapore's National Day (09 Aug) fell on a Sunday. Default is NO
# substitution — most of continental Europe simply loses the holiday —
# so we only claim a market is closed where the practice is real.
_IN_LIEU_NEXT_MONDAY = {
    'Singapore', 'Malaysia', 'Hong Kong', 'UK', 'Ireland', 'Nigeria',
    'Kenya', 'South Africa', 'India', 'Philippines', 'Indonesia',
    'Australia', 'New Zealand', 'Canada', 'Japan', 'South Korea',
    'Taiwan', 'Thailand', 'Vietnam', 'Sri Lanka', 'Bangladesh',
    'Pakistan',
}
# The US moves a Saturday holiday BACK to Friday and a Sunday holiday
# forward to Monday.
_IN_LIEU_NEAREST_WEEKDAY = {'US'}

_EASTER_OFFSETS = {
    'Maundy Thursday': -3, 'Good Friday': -2, 'Easter Saturday': -1,
    'Easter Monday': 1, 'Ascension': 39, 'Whit Monday': 50,
    'Pentecost Monday': 50, 'Corpus Christi': 60,
}


def _easter_sunday(year: int) -> _date:
    """Anonymous Gregorian algorithm — same one the dashboard JS uses."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return _date(year, month, day)


def _easter_date(year: int, name: str):
    off = _EASTER_OFFSETS.get(name)
    if off is None:
        return None
    return _easter_sunday(year) + _timedelta(days=off)


def _country_for(exchange: str):
    return EXCHANGE_COUNTRY.get((exchange or "").upper())


def _base_holiday_name(country: str, d: _date):
    """Holiday falling ON date `d` (no in-lieu shifting)."""
    entries = EXCHANGE_HOLIDAYS.get(country)
    if not entries:
        return None
    mmdd = d.strftime("%m-%d")
    ymd = d.strftime("%Y-%m-%d")
    for h in entries:
        date_part, _, name_part = h.partition(":")
        if date_part in (mmdd, ymd):
            return name_part or "holiday"
        if name_part and name_part in _EASTER_OFFSETS:
            ed = _easter_date(d.year, name_part)
            if ed == d:
                return name_part
    return None


def holiday_name(exchange: str, d: _date):
    """Name of the trading holiday closing `exchange` on `d`, else None.

    Handles the day-in-lieu case: a holiday landing on a weekend closes
    the following Monday in most Commonwealth/Asian markets (and the
    adjacent weekday in the US). Without this, Singapore's National Day
    on Sunday 09 Aug 2026 left Monday the 10th looking like a normal
    trading day, so every SGX price sat flat at 0.00% with no
    explanation.
    """
    country = _country_for(exchange)
    if not country:
        return None
    direct = _base_holiday_name(country, d)
    if direct:
        return direct
    # Monday standing in for a Saturday/Sunday holiday.
    if country in _IN_LIEU_NEXT_MONDAY and d.weekday() == 0:
        for back in (1, 2):          # Sunday, then Saturday
            src = d - _timedelta(days=back)
            nm = _base_holiday_name(country, src)
            if nm:
                return f"{nm} (observed)"
    if country in _IN_LIEU_NEAREST_WEEKDAY:
        if d.weekday() == 0:         # Monday for a Sunday holiday
            nm = _base_holiday_name(country, d - _timedelta(days=1))
            if nm:
                return f"{nm} (observed)"
        if d.weekday() == 4:         # Friday for a Saturday holiday
            nm = _base_holiday_name(country, d + _timedelta(days=1))
            if nm:
                return f"{nm} (observed)"
    return None


def is_holiday(exchange: str, d: _date) -> bool:
    return holiday_name(exchange, d) is not None
