START TRANSACTION;

CREATE TABLE IF NOT EXISTS user (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(32) NOT NULL,
    email VARCHAR(255) NOT NULL,
    profile_pic VARCHAR(255),
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP
);
    
INSERT INTO user (name, email, profile_pic) VALUES ('Charles Hughes', 'charles_hughes@catalystgrowth.info', 'https://randomuser.me/api/portraits/men/97.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Helen Abbott', 'helen_abbott@coreachievers.co', 'https://randomuser.me/api/portraits/men/139.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Joyce Cook', 'joyce_cook@brightfuturesolutions.tech', 'https://randomuser.me/api/portraits/women/64.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Janet Bishop', 'janet_bishop@apexinnovate.tech', 'https://randomuser.me/api/portraits/women/27.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Babara Crump', 'babara_crump@prosolutions.com', 'https://randomuser.me/api/portraits/men/121.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Brenda Peak', 'brenda_peak@summitascent.org', 'https://randomuser.me/api/portraits/women/65.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Betty Little', 'betty_little@strategicedge.net', 'https://randomuser.me/api/portraits/men/17.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Lauren Gallegos', 'lauren_gallegos@primeadvisors.info', 'https://randomuser.me/api/portraits/men/20.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Herman Dull', 'herman_dull@optimindgroup.biz', 'https://randomuser.me/api/portraits/women/62.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Rebecca Ogren', 'rebecca_ogren@globalimpact.co', 'https://randomuser.me/api/portraits/women/18.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Leroy Peters', 'leroy_peters@brightfuturesolutions.tech', 'https://randomuser.me/api/portraits/men/75.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Ellen Frink', 'ellen_frink@summitascent.org', 'https://randomuser.me/api/portraits/women/29.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Portia Snyder', 'portia_snyder@infinitepossibilities.biz', 'https://randomuser.me/api/portraits/men/33.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Lorene Murray', 'lorene_murray@elevatesuccess.org', 'https://randomuser.me/api/portraits/women/126.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Christopher Hart', 'christopher_hart@summitascent.org', 'https://randomuser.me/api/portraits/men/132.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Sharon Partain', 'sharon_partain@instagram.com', 'https://randomuser.me/api/portraits/women/82.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Thomas Vanhofwegen', 'thomas_vanhofwegen@summitascent.org', 'https://randomuser.me/api/portraits/men/25.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Eduardo Bode', 'eduardo_bode@premieralliance.com', 'https://randomuser.me/api/portraits/men/107.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Vita Campion', 'vita_campion@summitascent.org', 'https://randomuser.me/api/portraits/men/130.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Emily Shelly', 'emily_shelly@futureforward.net', 'https://randomuser.me/api/portraits/women/56.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Don Thomas', 'don_thomas@instagram.com', 'https://randomuser.me/api/portraits/men/123.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Janice Napier', 'janice_napier@spotify.com', 'https://randomuser.me/api/portraits/men/1.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Wallace Duffy', 'wallace_duffy@masterfulresults.net', 'https://randomuser.me/api/portraits/men/105.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Marvel Johnson', 'marvel_johnson@coreachievers.co', 'https://randomuser.me/api/portraits/men/61.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Ferdinand Sims', 'ferdinand_sims@venturepinnacle.io', 'https://randomuser.me/api/portraits/women/43.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Robert Koons', 'robert_koons@apexinnovate.tech', 'https://randomuser.me/api/portraits/men/65.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Wallace Evans', 'wallace_evans@venturepinnacle.io', 'https://randomuser.me/api/portraits/women/24.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Everett Marshall', 'everett_marshall@strategicedge.net', 'https://randomuser.me/api/portraits/women/24.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Roy Welle', 'roy_welle@quantumleap.com', 'https://randomuser.me/api/portraits/women/112.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Rubin Barnhill', 'rubin_barnhill@coreachievers.co', 'https://randomuser.me/api/portraits/men/149.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Richard Johnson', 'richard_johnson@elevatesuccess.org', 'https://randomuser.me/api/portraits/women/12.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Scott Heifner', 'scott_heifner@futureforward.net', 'https://randomuser.me/api/portraits/men/50.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Lakisha Guercio', 'lakisha_guercio@catalystgrowth.info', 'https://randomuser.me/api/portraits/women/88.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Michael Pierre', 'michael_pierre@synergypath.io', 'https://randomuser.me/api/portraits/men/102.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Paul Roberts', 'paul_roberts@elevatesuccess.org', 'https://randomuser.me/api/portraits/women/65.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Angela Rice', 'angela_rice@synergypath.io', 'https://randomuser.me/api/portraits/women/8.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Lucy Beck', 'lucy_beck@instagram.com', 'https://randomuser.me/api/portraits/women/101.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Mike Ortega', 'mike_ortega@infinitepossibilities.biz', 'https://randomuser.me/api/portraits/men/71.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Warren Yates', 'warren_yates@apexinnovate.tech', 'https://randomuser.me/api/portraits/women/116.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Kendrick Weeks', 'kendrick_weeks@catalystgrowth.info', 'https://randomuser.me/api/portraits/men/138.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Paul Ward', 'paul_ward@masterfulresults.net', 'https://randomuser.me/api/portraits/men/145.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Richard Adair', 'richard_adair@instagram.com', 'https://randomuser.me/api/portraits/men/79.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Mary Schrunk', 'mary_schrunk@gmail.com', 'https://randomuser.me/api/portraits/men/6.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Jamie Ellsworth', 'jamie_ellsworth@catalystgrowth.info', 'https://randomuser.me/api/portraits/women/53.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Candice Dubinsky', 'candice_dubinsky@apexinnovate.tech', 'https://randomuser.me/api/portraits/men/8.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Ilana Nims', 'ilana_nims@truenorthconsult.co', 'https://randomuser.me/api/portraits/men/31.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Victoria Clines', 'victoria_clines@gmail.com', 'https://randomuser.me/api/portraits/women/104.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Albert Gobel', 'albert_gobel@venturepinnacle.io', 'https://randomuser.me/api/portraits/women/54.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Robert Ferguson', 'robert_ferguson@premieralliance.com', 'https://randomuser.me/api/portraits/men/14.jpg');
INSERT INTO user (name, email, profile_pic) VALUES ('Joseph Scheidecker', 'joseph_scheidecker@gmail.com', 'https://randomuser.me/api/portraits/men/106.jpg');
COMMIT;
