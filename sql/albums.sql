START TRANSACTION;

CREATE TABLE IF NOT EXISTS album (
    id INT PRIMARY KEY AUTO_INCREMENT,
    artist_id INT NOT NULL,
    title VARCHAR(100) NOT NULL,
    cover_pic VARCHAR(255),
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY(artist_id)
        REFERENCES artist(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (1, 'Renditions', 'https://i.scdn.co/image/ab67616d0000b2735be5e649385db0e7bafdd0bd', '2014-04-24 23:33:09');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (8, 'A New Day', 'https://i.scdn.co/image/ab67616d0000b27377e4d763b642882770fcab53', '2024-11-29 03:46:52');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (12, 'Renditions, Pt. 2', 'https://i.scdn.co/image/ab67616d0000b2735601a443ebbb9f9ecaa99ca5', '2018-02-23 13:34:39');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (11, 'B-Sides', 'https://i.scdn.co/image/ab67616d0000b2738690e28de38cb5f78e2caec6', '2019-08-09 00:16:42');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (15, 'Daydreaming', 'https://i.scdn.co/image/ab67616d0000b2731f323638a085588c7b3b395d', '2022-02-25 17:06:53');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Drops (Ao Vivo)', 'https://i.scdn.co/image/ab67616d0000b27315384b1ca67affc3dc8a200f', '2016-12-10 11:26:07');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (19, 'Drip da Roça', 'https://i.scdn.co/image/ab67616d0000b273849ae3155cf185e2a6ec4979', '2020-04-17 03:35:11');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (7, 'Dripped', 'https://i.scdn.co/image/ab67616d0000b2739410ecdef0fc6e95273100a7', '2024-07-02 08:05:21');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (15, 'Saviors', 'https://i.scdn.co/image/ab67616d0000b2734e805200727de6c107de5b33', '2024-01-19 09:37:32');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (6, 'Certain Things (feat. Chasing Grace) [Sped Up]', 'https://i.scdn.co/image/ab67616d0000b2735c60c3a15d8b9f15dcc7dedb', '2013-11-04 18:08:39');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (3, 'Play Deep', 'https://i.scdn.co/image/ab67616d0000b273cd7e670a91d0263d2ee1d8a7', '1985-06-29 10:35:25');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (13, 'Information Society', 'https://i.scdn.co/image/ab67616d0000b273e4263dbad5243053ec4a8470', '1988-06-21 04:35:31');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (16, 'To Build A Home (Versions)', 'https://i.scdn.co/image/ab67616d0000b2730eb232a1b19b8c8139645a82', '2007-10-29 05:25:52');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Build It Better', 'https://i.scdn.co/image/ab67616d0000b2735725506f108b0d72912d60f7', '2015-03-19 17:10:49');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (9, 'Again (feat. XXXTENTACION)', 'https://i.scdn.co/image/ab67616d0000b27349af7d5bfd29ae589a99e1cd', '2017-09-22 12:32:47');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (11, 'Again', 'https://i.scdn.co/image/ab67616d0000b27381bb674330f0276e88bd0722', '2000-01-01 23:36:50');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Candle Burning', 'https://i.scdn.co/image/ab67616d0000b273d487b00796738119685ea1f7', '2025-05-23 16:15:08');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (8, 'Hebron Gate', 'https://i.scdn.co/image/ab67616d0000b273d4c803969da649e554a660f0', '2002-10-05 12:12:04');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (11, 'Each One Teach One', 'https://i.scdn.co/image/ab67616d0000b2733547034c8967b828a96412bf', '2018-03-16 01:18:47');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (9, 'Here I Am', 'https://i.scdn.co/image/ab67616d0000b27390162d6aad19f75ec4a21037', '2009-07-28 16:02:51');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (9, 'HELLO FUNK', 'https://i.scdn.co/image/ab67616d0000b273a1fb4b7fba4bad325c7d49ad', '2025-04-25 17:12:14');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (5, 'Made In Favela', 'https://i.scdn.co/image/ab67616d0000b273e6e4c758782e16dfacbfb3df', '2025-04-10 12:51:05');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (12, 'Made To Feel', 'https://i.scdn.co/image/ab67616d0000b273f249990c726499ab31b0c5c6', '2023-10-20 02:51:54');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (20, 'Made It', 'https://i.scdn.co/image/ab67616d0000b273f167f1ca3c2de6c415352db6', '2025-03-08 11:40:39');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (10, 'Hitmado 2', 'https://i.scdn.co/image/ab67616d0000b27352a9bc52729a7659b25fe24e', '2023-12-19 05:52:50');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (14, 'Call Me', 'https://i.scdn.co/image/ab67616d0000b27313cbe3e4dfa81fe5b91f2f1c', '2004-01-01 14:33:29');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (14, 'MONTERO (Call Me By Your Name)', 'https://i.scdn.co/image/ab67616d0000b273664034dd80e91b28f773598d', '2021-03-31 09:50:40');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (15, 'Information Society', 'https://i.scdn.co/image/ab67616d0000b273e4263dbad5243053ec4a8470', '1988-06-21 11:07:21');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (19, 'Call Me (Slowed)', 'https://i.scdn.co/image/ab67616d0000b273af279eb21d4842eae7066f36', '2021-10-06 16:09:51');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (11, 'Brand New Eyes', 'https://i.scdn.co/image/ab67616d0000b273e01d7d558032457b0e4883f6', '2009-09-22 08:14:13');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (14, 'Brand New Day', 'https://i.scdn.co/image/ab67616d0000b273db85318130166610d4095c51', '1999-09-24 09:37:55');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (13, 'A Brand New Me: Aretha Franklin (with The Royal Philharmonic Orchestra)', 'https://i.scdn.co/image/ab67616d0000b273d4750565018a7842b38914df', '2017-11-10 19:50:39');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (6, 'Metal Health', 'https://i.scdn.co/image/ab67616d0000b27321fbdd09672c5f8bcfd66165', '1983-05-08 14:45:52');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (20, 'Quiet Moments', 'https://i.scdn.co/image/ab67616d0000b273c0ffba18b0509931ff97967c', '2022-07-08 17:49:30');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (5, 'Quiet Nights (Slowed and Reverb)', 'https://i.scdn.co/image/ab67616d0000b273c9157ffade0e1250bdc20aab', '2024-10-04 17:17:49');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (6, 'Quietud Serena', 'https://i.scdn.co/image/ab67616d0000b273b6baf520ec94028c8342a7df', '2023-07-24 05:35:33');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (11, 'Stay on These Roads', 'https://i.scdn.co/image/ab67616d0000b27310816799318a67035593e8df', '1988-05-03 17:31:14');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (18, 'Bright Lights (Lights Re-pack / Bonus Version)', 'https://i.scdn.co/image/ab67616d0000b273512cd44d21eacbfeda4affe2', '2010-01-01 10:05:08');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (3, 'Brylho', 'https://i.scdn.co/image/ab67616d0000b273c40966679cbd96fa47e0e59c', '1983-04-27 18:52:22');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (1, 'Sunshine', 'https://i.scdn.co/image/ab67616d0000b27391c55bc3286e47a3ed122a4d', '2017-12-01 04:52:53');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Sunshine (My Girl)', 'https://i.scdn.co/image/ab67616d0000b27342401c4a9e13cddc69e34ffd', '2023-09-01 17:46:54');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Ain''t No Sunshine (feat. Jasmine Pace)', 'https://i.scdn.co/image/ab67616d0000b2730b5d12c0b5dac5c99f181d52', '2021-07-30 03:05:43');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (14, 'Lights Are On (Instrumental)', 'https://i.scdn.co/image/ab67616d0000b27349f70a853013632d07743d9f', '2021-11-05 18:51:16');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'Flashback', 'https://i.scdn.co/image/ab67616d0000b273f3489fa9edf77c90aa8ac28f', '2009-01-01 21:28:49');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (9, 'Lights (Sped Up Version)', 'https://i.scdn.co/image/ab67616d0000b273bb4cc06a0334303c3684b776', '2022-07-01 07:21:01');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (14, 'The Best Of Men At Work: Contraband', 'https://i.scdn.co/image/ab67616d0000b273d6a1f7a12629154fa274631f', '1996-02-01 06:31:32');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (7, 'Stay on These Roads', 'https://i.scdn.co/image/ab67616d0000b27310816799318a67035593e8df', '1988-05-03 21:53:20');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (9, 'Take On Me (2017 Acoustic)', 'https://i.scdn.co/image/ab67616d0000b273db1496b5e1d605c2fd7d70d9', '2017-12-01 04:09:02');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (10, 'The Age of Consent', 'https://i.scdn.co/image/ab67616d0000b2732ec4aea38b44eba9756767fa', '1984-10-15 23:58:04');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (7, 'Scoundrel Days (Deluxe Edition)', 'https://i.scdn.co/image/ab67616d0000b2739126fb3b1b0c4ec47ab659b5', '1986-10-06 10:45:11');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (10, 'Noel', 'https://i.scdn.co/image/ab67616d0000b273d0597a27884b22e6f16ef1b0', '1988-03-07 08:18:54');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'História de Amor', 'https://i.scdn.co/image/ab67616d0000b2730c0fb4923347d6f229f16df7', '2015-11-07 00:43:20');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Climbing!', 'https://i.scdn.co/image/ab67616d0000b273411cacfc3518a9f2c4a670f6', '1970-05-13 22:16:11');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (1, 'Mountains', 'https://i.scdn.co/image/ab67616d0000b273f4796ade02dd06fd99ef2e23', '2024-11-08 11:05:57');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (8, 'Mountain Air', 'https://i.scdn.co/image/ab67616d0000b273453ac1e1f815d00be11b5828', '2025-06-05 01:17:38');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (12, 'Ain''t No Mountain High Enough', 'https://i.scdn.co/image/ab67616d0000b273162d1559879e0cfec3e9dd22', '2025-02-14 00:44:22');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (10, 'Conselho da Minha Ex', 'https://i.scdn.co/image/ab67616d0000b273bb4aa27a74bbde6e76f85be2', '2023-08-11 07:44:18');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (1, 'Esencia Profunda', 'https://i.scdn.co/image/ab67616d0000b273da15f473a8a8c0ce11218101', '2025-06-02 17:13:59');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (8, 'Consequências De Um Coração Machucado', 'https://i.scdn.co/image/ab67616d0000b273221b55f4ee769994c6b41085', '2023-10-05 23:46:42');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (12, 'Xonei', 'https://i.scdn.co/image/ab67616d0000b273f5fb08798b3beb3633eba99f', '2025-03-28 15:32:28');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'Stay on These Roads', 'https://i.scdn.co/image/ab67616d0000b27310816799318a67035593e8df', '1988-05-03 18:21:35');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (17, 'ドライブが楽しくなる洋楽ヒッツ！', 'https://i.scdn.co/image/ab67616d0000b2730946d11271fa6083bc8ea769', '2025-05-02 05:13:01');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (6, 'Intellect', 'https://i.scdn.co/image/ab67616d0000b2739108ab60791a5b4e8a11cd98', '2024-01-09 04:35:43');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (17, 'Memory', 'https://i.scdn.co/image/ab67616d0000b273b7d0b122e020c571d73860ab', '2024-01-12 20:56:05');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (7, 'Creep EP', 'https://i.scdn.co/image/ab67616d0000b2733340ec817b195f40615cc647', '1992-09-21 18:21:43');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (16, 'Creep', 'https://i.scdn.co/image/ab67616d0000b27337f1a4721cee2084122a4960', '2022-08-05 16:58:52');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (10, 'Carlos, Erasmo [Versão Com Bônus (1971)]', 'https://i.scdn.co/image/ab67616d0000b2731d043e95299fc3af12d997b7', '1971-01-01 23:29:02');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (5, 'Collected', 'https://i.scdn.co/image/ab67616d0000b2737f29399efc7ee01e8c56936d', '2018-05-25 17:52:02');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'H/lf W/y', 'https://i.scdn.co/image/ab67616d0000b27349a2cfb34fa27f3b4ac912b3', '2025-01-24 01:45:44');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (17, 'Bença', 'https://i.scdn.co/image/ab67616d0000b27335c77d27d6afa42d05c88ff1', '2024-05-17 14:57:30');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (3, 'Collateral Damage', 'https://i.scdn.co/image/ab67616d0000b2738c3ac36f04030bba19d8f3b6', '2022-03-18 06:49:47');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (17, 'Collateral Beauty (Original Motion Picture Soundtrack)', 'https://i.scdn.co/image/ab67616d0000b27332ca89cb62540e536556daa8', '2016-12-09 21:54:16');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (14, 'Born This Way (International Special Edition Version)', 'https://i.scdn.co/image/ab67616d0000b273a5d31644260279be8d0c46c0', '2011-05-23 00:45:14');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Born In The U.S.A.', 'https://i.scdn.co/image/ab67616d0000b273a7865e686c36a4adda6c9978', '1984-06-04 20:44:20');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (12, 'Chain React', 'https://i.scdn.co/image/ab67616d0000b273b713e926e24e7076129db185', '2025-05-08 20:05:33');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (17, 'Asia', 'https://i.scdn.co/image/ab67616d0000b2732323f86e757c3436b3cc38af', '1982-03-08 15:11:34');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (6, 'Play Station', 'https://i.scdn.co/image/ab67616d0000b27323ef588f5e8caf79bf497151', '2025-05-29 14:14:48');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (3, 'Born In Babylon', 'https://i.scdn.co/image/ab67616d0000b2731bdd06e125e445368be9c371', '2009-08-25 12:02:37');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'JAVA', 'https://i.scdn.co/image/ab67616d0000b2732950b6ef1e1af078dc3cf098', '2025-04-13 07:55:44');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (6, 'Amid the Noise and Haste', 'https://i.scdn.co/image/ab67616d0000b273cf0b04addf3c14a703c04801', '2014-08-12 04:22:27');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (18, 'Sky Full Of Noise', 'https://i.scdn.co/image/ab67616d0000b273f1b35251884b0598b1f72f26', '2022-03-22 10:35:44');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (8, 'Sky High (feat. Nonô)', 'https://i.scdn.co/image/ab67616d0000b273cb129037000500f54d2e8bc6', '2021-12-31 03:16:17');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (17, 'Sky of Gentle Flames (Forest)', 'https://i.scdn.co/image/ab67616d0000b273e345e47d26d76289cc0545a1', '2025-02-10 04:21:29');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'SMILE!', 'https://i.scdn.co/image/ab67616d0000b27336522fd9ea16c612985a37bc', '2025-04-03 06:14:10');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Sorriso Resplandecente (from Dragon Ball GT)', 'https://i.scdn.co/image/ab67616d0000b273acc13da92e3cd3b8f89626d7', '2024-11-14 14:53:15');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (15, 'Miracles (Original Album)', 'https://i.scdn.co/image/ab67616d0000b2732c774e94228f684940908ef8', '1981-07-14 20:59:51');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'The Glow of Love (Original Album and Rare Tracks)', 'https://i.scdn.co/image/ab67616d0000b273787279160a62de850ebdb7b3', '1980-05-11 13:33:56');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (17, 'Changes', 'https://i.scdn.co/image/ab67616d0000b2730dfa9b85163fea3050657c60', '1967-02-10 02:56:34');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (4, 'Jessé', 'https://i.scdn.co/image/ab67616d0000b273f4fcb18d80eeedbc004c1d29', '1980-01-01 18:40:01');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (18, 'Glow', 'https://i.scdn.co/image/ab67616d0000b273aa94ca8ca49c2ea033764aaa', '1976-06-15 02:49:47');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (8, 'Sorry (with Madonna)', 'https://i.scdn.co/image/ab67616d0000b273d215b03cc6554fd5ed4c8313', '2023-02-10 01:25:30');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (16, 'Sorry Marin', 'https://i.scdn.co/image/ab67616d0000b27362651dc13b168bc7588ed83a', '2004-03-16 19:30:45');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (20, 'Sorry Not Sorry (with Slash) [Rock Version]', 'https://i.scdn.co/image/ab67616d0000b273c22c152027a1f64aaf834b5e', '2023-07-14 23:11:51');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (11, 'Separate Ways (Worlds Apart) [Bryce Miller/Alloy Tracks Remix]', 'https://i.scdn.co/image/ab67616d0000b273cc20353b6f469858d50a0adf', '2022-04-12 13:20:23');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'We All Need Love', 'https://i.scdn.co/image/ab67616d0000b273672db21dd51ae877901129f7', '1992-01-01 13:08:54');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (15, 'Back To New Wave', 'https://i.scdn.co/image/ab67616d0000b2735f4ef49cdf4d53334baea8af', '1995-05-01 14:41:02');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (20, 'Back For Good', 'https://i.scdn.co/image/ab67616d0000b273dfc101f29525207c58e1ac8f', '1998-09-11 04:48:02');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (2, 'Drink', 'https://i.scdn.co/image/ab67616d0000b27317b2105c09f567e84348b6b9', '2025-04-11 22:32:28');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (14, 'Drinkee', 'https://i.scdn.co/image/ab67616d0000b2739d543da0baad4a7f6aeeefe6', '2021-07-23 22:33:06');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (16, 'Memories', 'https://i.scdn.co/image/ab67616d0000b2736007e91eac5f5b0d8c9ee0f4', '2017-11-17 22:21:12');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (15, 'A Kind Of Magic (2011 Remaster)', 'https://i.scdn.co/image/ab67616d0000b273e98d08fbea1a2f03229abc20', '1986-06-02 05:37:50');
INSERT INTO album (artist_id, title, cover_pic, created_at) VALUES (1, 'Mistaken Identity', 'https://i.scdn.co/image/ab67616d0000b2738ccc17f29764d812062204a8', '1981-01-01 08:15:09');
COMMIT;
