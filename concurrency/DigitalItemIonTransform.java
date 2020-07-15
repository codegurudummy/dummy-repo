// Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
package concurrency;

import amazon.dtpServiceTypes.Locale;
import amazon.dtpServiceTypes.*;
import amazon.platform.profiler.ProfilerScope;
import com.amazon.cardinal.client.CardinalRequest;
import com.amazon.cardinal.client.CardinalRequest.RequestType;
import com.amazon.cardinal.exception.NotFoundException;
import com.amazon.ion.*;
import com.amazon.ion.util.IonValueUtils;
import com.amazon.kdp.InterfaceTypeUtil;
import com.amazon.kdp.Singletons;
import com.amazon.kdp.cardinal.DigitalBookResource;
import com.amazon.kdp.cardinal.KDPItemSetResource;
import com.amazon.kdp.digitalbook.DopsData;
import com.amazon.nutrition.cast.IonTypeCast;
import com.amazon.nutrition.cast.IonValueCastException;
import com.amazon.nutrition.struct.IonStructs;
import com.amazon.nutrition.struct.TypeSafeIonStruct;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.ion.util.IonValueUtils.anyNull;
import static com.amazon.kdp.digitalbook.IonUtils.*;
import static com.amazon.kdp.digitalbook.transform.DigitalBookFields.*;

public final class DigitalItemIonTransform {

   private DigitalItemIonTransform() { }
   
   private static final IonSystem ION_SYSTEM = Singletons.getIonSystem();
   private static final Double ROUNDING_FACTOR = 100.0;
   public static final String KW_DATA_SDL = "KW.Book@0.1";
   
   private static ThreadLocal<Boolean> DOPS_DATA_CORRECTED = new ThreadLocal<>();
   private static Logger logger = Logger.getLogger(DigitalItemIonTransform.class);
   private static KDPItemSetResource KDP_ITEM_SET_RESOURCE = new KDPItemSetResource();
   private static DigitalBookResource DIGITAL_BOOK_RESOURCE = new DigitalBookResource();

   public static final Set<String> SALES_TERRITORIES = Stream
         .of("AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS",
               "AT", "AU", "AW", "AX", "AZ", "BA", "BB", "BD", "BE", "BF",
               "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BR", "BS",
               "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG",
               "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV",
               "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC",
               "EE", "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FK", "FM",
               "FO", "FR", "GA", "GB", "GD", "GE", "GF", "GG", "GH", "GI",
               "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW",
               "GY", "HK", "HM", "HN", "HR", "HT", "HU", "ID", "IE", "IL",
               "IM", "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM", "JO",
               "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW",
               "KY", "KZ", "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT",
               "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH",
               "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT",
               "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NE", "NF",
               "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA",
               "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR", "PS",
               "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW", "SA",
               "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL",
               "SM", "SN", "SO", "SR", "ST", "SV", "SY", "SZ", "TC", "TD",
               "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TR",
               "TT", "TV", "TW", "TZ", "UA", "UG", "UM", "US", "UY", "UZ",
               "VA", "VC", "VE", "VG", "VI", "VN", "VU", "WF", "WS", "YE",
               "YT", "ZA", "ZM", "ZW").collect(Collectors.toSet());

   public static final BiMap<String, String> KDP_CONTRIBUTOR_ROLE_CODES = HashBiMap
         .create(new HashMap<String, String>() {
            {
               put("AUTHOR", "A01");
               put("EDITOR", "B21");
               put("FOREWORD", "A23");
               put("ILLUSTRATOR", "A12");
               put("INTRODUCTION", "A24");
               put("NARRATOR", "E03");
               put("PHOTOGRAPHER", "A13");
               put("PREFACE", "A15");
               put("TRANSLATOR", "B06");
            }
         });
  
   // CHECKSTYLE:SUPPRESS:MagicNumber
   public static final BiMap<String, Double> ROYALTY_PLAN_TO_RATE_MAPPING = HashBiMap
         .create(new HashMap<String, Double>() {
            {
               put("35_PERCENT", 35.0);
               put("50_PERCENT", 50.0);
               put("70_PERCENT", 70.0);
            }
         });

   public static final BiMap<String, String> LANGUAGE_CODES = HashBiMap
         .create(new HashMap<String, String>() {
            {
               put("aar", "afar");
               put("abk", "abkhazian");
               put("ace", "achinese");
               put("ach", "acoli");
               put("ada", "adangme");
               put("ady", "adygei");
               put("afa", "afro_asiatic_languages");
               put("afh", "afrihili");
               put("afr", "afrikaans");
               put("ain", "ainu");
               put("aka", "akan");
               put("akk", "akkadian");
               put("ale", "aleut");
               put("alg", "algonquian_languages");
               put("alt", "southern_altai");
               put("amh", "amharic");
               put("ang", "old_english");
               put("anp", "angika");
               put("apa", "apache_languages");
               put("ara", "arabic");
               put("arc", "aramaic");
               put("arg", "aragonese");
               put("arn", "mapudungun");
               put("arp", "arapaho");
               put("art", "artificial_languages");
               put("arw", "arawak");
               put("asm", "assamese");
               put("ast", "asturian");
               put("ath", "athapascan_languages");
               put("aus", "australian_languages");
               put("ava", "avaric");
               put("ave", "avestan");
               put("awa", "awadhi");
               put("aym", "aymara");
               put("aze", "azerbaijani");
               put("bad", "banda_languages");
               put("bai", "bamileke_languages");
               put("bak", "bashkir");
               put("bal", "baluchi");
               put("bam", "bambara");
               put("ban", "balinese");
               put("bas", "basa");
               put("bat", "baltic_languages");
               put("bej", "beja");
               put("bel", "byelorussian");
               put("bem", "bemba");
               put("ben", "bengali");
               put("ber", "berber");
               put("bho", "bhojpuri");
               put("bih", "bihari");
               put("bik", "bikol");
               put("bin", "bini");
               put("bis", "bislama");
               put("bla", "siksika");
               put("bnt", "bantu");
               put("bod", "tibetan");
               put("bos", "bosnian");
               put("bra", "braj");
               put("bre", "breton");
               put("btk", "batak");
               put("bua", "buryat");
               put("bug", "buginese");
               put("bul", "bulgarian");
               put("byn", "blin");
               put("cad", "caddo");
               put("cai", "central_american_indian_languages");
               put("car", "galibi_carib");
               put("cat", "catalan");
               put("cau", "caucasian_languages");
               put("ceb", "cebuano");
               put("cel", "celtic_languages");
               put("ces", "czech");
               put("cha", "chamorro");
               put("chb", "chibcha");
               put("che", "chechen");
               put("chg", "chagatai");
               put("chk", "chuukese");
               put("chm", "mari");
               put("chn", "chinook_jargon");
               put("cho", "choctaw");
               put("chp", "chipewyan");
               put("chr", "cherokee");
               put("chu", "old_slavonic");
               put("chv", "chuvash");
               put("chy", "cheyenne");
               put("cmc", "chamic_languages");
               put("cop", "coptic");
               put("cor", "cornish");
               put("cos", "corsican");
               put("cpe", "gullah");
               put("cpf", "french_creole");
               put("cpp", "portuguese_creole");
               put("cre", "cree");
               put("crh", "crimean_tatar");
               put("crp", "creole");
               put("csb", "kashubian");
               put("cus", "cushitic_languages");
               put("cym", "welsh");
               put("dak", "dakota");
               put("dan", "danish");
               put("dar", "dargwa");
               put("day", "land_dayak_languages");
               put("del", "delaware");
               put("den", "slave_athapascan");
               put("deu", "german");
               put("dgr", "dogrib");
               put("din", "dinka");
               put("div", "dhivehi");
               put("doi", "dogri");
               put("dra", "dravidian_languages");
               put("dsb", "lower_sorbian");
               put("dua", "duala");
               put("dum", "middle_dutch");
               put("dyu", "jula");
               put("dzo", "dzongkha");
               put("efi", "efik");
               put("egy", "egyptian");
               put("eka", "ekajuk");
               put("ell", "greek");
               put("elx", "elamite");
               put("eng", "english");
               put("enm", "middle_english");
               put("epo", "esperanto");
               put("est", "estonian");
               put("eus", "basque");
               put("ewe", "ewe");
               put("ewo", "ewondo");
               put("fan", "fang");
               put("fao", "faroese");
               put("fas", "persian");
               put("fat", "fanti");
               put("fij", "fiji");
               put("fil", "filipino");
               put("fin", "finnish");
               put("fiu", "finno_ugrian");
               put("fon", "fon");
               put("fra", "french");
               put("frm", "middle_french");
               put("fro", "old_french");
               put("frp", "provencal");
               put("frr", "northern_frisian");
               put("frs", "eastern_frisian");
               put("fry", "frisian");
               put("ful", "fulah");
               put("fur", "friulian");
               put("gaa", "ga");
               put("gay", "gayo");
               put("gba", "gbaya");
               put("gem", "germanic_languages");
               put("gez", "geez");
               put("gil", "gilbertese");
               put("gla", "scots_gaelic");
               put("gle", "irish");
               put("glg", "galician");
               put("glv", "manx");
               put("gmh", "middle_high_german");
               put("goh", "old_high_german");
               put("gon", "gondi");
               put("gor", "gorontalo");
               put("got", "gothic");
               put("grb", "grebo");
               put("grc", "ancient_greek");
               put("grn", "guarani");
               put("gsw", "swiss_german");
               put("guj", "gujarati");
               put("gwi", "gwichin");
               put("hai", "haida");
               put("hat", "haitian");
               put("hau", "hausa");
               put("haw", "hawaiian");
               put("heb", "hebrew");
               put("her", "herero");
               put("hil", "hiligaynon");
               put("him", "himachali_languages");
               put("hin", "hindi");
               put("hit", "hittite");
               put("hmn", "hmong");
               put("hmo", "hiri_motu");
               put("hrv", "croatian");
               put("hsb", "upper_sorbian");
               put("hun", "hungarian");
               put("hup", "hupa");
               put("hye", "armenian");
               put("iba", "iban");
               put("ibo", "igbo");
               put("ido", "ido");
               put("iii", "sichuan_yi");
               put("ijo", "ijo_languages");
               put("iku", "inuktitut");
               put("ile", "interlingue");
               put("ilo", "iloko");
               put("ina", "interlingua");
               put("inc", "indic");
               put("ind", "indonesian");
               put("ine", "indo_european");
               put("inh", "ingush");
               put("ipk", "inupiaq");
               put("ira", "iranian_languages");
               put("iro", "iroquoian_languages");
               put("isl", "icelandic");
               put("ita", "italian");
               put("jav", "javanese");
               put("jbo", "lojban");
               put("jpn", "japanese");
               put("jpr", "judeo_persian");
               put("jrb", "judeo_arabic");
               put("kaa", "kara_kalpak");
               put("kab", "kabyle");
               put("kac", "kachin");
               put("kal", "kalaallisut");
               put("kam", "kamba");
               put("kan", "kannada");
               put("kar", "karen_languages");
               put("kas", "kashmiri");
               put("kat", "georgian");
               put("kau", "kanuri");
               put("kaw", "kawi");
               put("kaz", "kazakh");
               put("kbd", "kabardian");
               put("kha", "khasi");
               put("khi", "khoisan_languages");
               put("khm", "khmer");
               put("kho", "khotanese");
               put("kik", "kikuyu");
               put("kin", "kinyarwanda");
               put("kir", "kirghiz");
               put("kmb", "kimbundu");
               put("kok", "konkani");
               put("kom", "komi");
               put("kon", "kongo");
               put("kor", "korean");
               put("kos", "kosraean");
               put("kpe", "kpelle");
               put("krc", "karachay_balkar");
               put("krl", "karelian");
               put("kro", "kru_languages");
               put("kru", "kurukh");
               put("kua", "kuanyama");
               put("kum", "kumyk");
               put("kur", "kurdish");
               put("kut", "kutenai");
               put("lad", "ladino");
               put("lah", "lahnda");
               put("lam", "lamba");
               put("lao", "lao");
               put("lat", "latin");
               put("lav", "latvian");
               put("lez", "lezghian");
               put("lim", "limburgan");
               put("lin", "lingala");
               put("lit", "lithuanian");
               put("lol", "mongo");
               put("loz", "lozi");
               put("ltz", "luxembourgish");
               put("lua", "luba_lulua");
               put("lub", "luba_katanga");
               put("lug", "ganda");
               put("lui", "luiseno");
               put("lun", "lunda");
               put("luo", "luo");
               put("lus", "lushai");
               put("mad", "madurese");
               put("mag", "magahi");
               put("mah", "marshallese");
               put("mai", "maithili");
               put("mak", "makasar");
               put("mal", "malayalam");
               put("man", "mandingo");
               put("map", "austronesian");
               put("mar", "marathi");
               put("mas", "masai");
               put("mdf", "moksha");
               put("mdr", "mandar");
               put("men", "mende");
               put("mga", "middle_irish");
               put("mic", "micmac");
               put("min", "minangkabau");
               put("mkd", "macedonian");
               put("mkh", "mon_khmer_languages");
               put("mlg", "malagasy");
               put("mlt", "maltese");
               put("mnc", "manchu");
               put("mni", "manipuri");
               put("mno", "manobo_languages");
               put("moh", "mohawk");
               put("mol", "moldavian");
               put("mon", "mongolian");
               put("mos", "mossi");
               put("mri", "maori");
               put("msa", "malay");
               put("mul", "multilingual");
               put("mun", "munda_languages");
               put("mus", "creek");
               put("mwl", "mirandese");
               put("mwr", "marwari");
               put("mya", "burmese");
               put("myn", "mayan");
               put("myv", "erzya");
               put("nah", "nahautl");
               put("nai", "north_american_indian_languages");
               put("nap", "neapolitan");
               put("nau", "nauru");
               put("nav", "navaho");
               put("nbl", "south_ndebele");
               put("nde", "north_ndebele");
               put("ndo", "ndonga");
               put("nds", "low_german");
               put("nep", "nepali");
               put("new", "nepal_bhasa");
               put("nia", "nias");
               put("nic", "niger_kordofanian_languages");
               put("niu", "niuean");
               put("nld", "dutch");
               put("nno", "norwegian_nynorsk");
               put("nob", "norwegian_bokmal");
               put("nog", "nogai");
               put("non", "old_norse");
               put("nor", "norwegian");
               put("nqo", "nko");
               put("nso", "pedi");
               put("nub", "nubian_languages");
               put("nwc", "classical_newari");
               put("nya", "chichewa");
               put("nym", "nyamwezi");
               put("nyn", "nyankole");
               put("nyo", "nyoro");
               put("nzi", "nzima");
               put("oci", "occitan");
               put("oji", "ojibwa");
               put("ori", "oriya");
               put("orm", "oromo");
               put("osa", "osage");
               put("oss", "ossetian");
               put("ota", "ottoman_turkish");
               put("oto", "otomian_languages");
               put("paa", "papuan_languages");
               put("pag", "pangasinan");
               put("pal", "pahlavi");
               put("pam", "pampanga");
               put("pan", "punjabi");
               put("pap", "papiamento");
               put("pau", "palauan");
               put("peo", "old_persian");
               put("phi", "philippine_languages");
               put("phn", "phoenician");
               put("pli", "pali");
               put("pol", "polish");
               put("pon", "pohnpeian");
               put("por", "portuguese");
               put("pra", "prakrit_languages");
               put("pro", "old_provencal");
               put("pus", "pashto");
               put("que", "quechua");
               put("raj", "rajasthani");
               put("rap", "rapanui");
               put("rar", "rarotongan");
               put("roa", "romance");
               put("roh", "romansh");
               put("rom", "romany");
               put("ron", "romanian");
               put("run", "kirundi");
               put("rup", "aromanian");
               put("rus", "russian");
               put("sad", "sandawe");
               put("sag", "sangho");
               put("sah", "yakut");
               put("sai", "south_american_indian");
               put("sal", "salishan_languages");
               put("sam", "samaritan");
               put("san", "sanskrit");
               put("sas", "sasak");
               put("sat", "santali");
               put("scn", "sicilian");
               put("sco", "scots");
               put("sel", "selkup");
               put("sem", "semitic_languages");
               put("sga", "old_irish");
               put("sgn", "sign_language");
               put("shn", "shan");
               put("sid", "sidamo");
               put("sin", "sinhalese");
               put("sio", "siouan_languages");
               put("sit", "sino_tibetan");
               put("sla", "slavic");
               put("slk", "slovak");
               put("slv", "slovene");
               put("sma", "southern_sami");
               put("sme", "northern_sami");
               put("smi", "sami");
               put("smj", "lule_sami");
               put("smn", "inari_sami");
               put("smo", "samoan");
               put("sms", "skolt_sami");
               put("sna", "shona");
               put("snd", "sindhi");
               put("snk", "soninke");
               put("sog", "sogdian");
               put("som", "somali");
               put("son", "songhai_languages");
               put("sot", "southern_sotho");
               put("spa", "spanish");
               put("sqi", "albanian");
               put("srd", "sardinian");
               put("srn", "sranan_tongo");
               put("srp", "serbian");
               put("srr", "serer");
               put("ssa", "nilo_saharan_languages");
               put("ssw", "siswati");
               put("suk", "sukuma");
               put("sun", "sundanese");
               put("sus", "susu");
               put("sux", "sumerian");
               put("swa", "swahili");
               put("swe", "swedish");
               put("syc", "classical_syriac");
               put("syr", "syriac");
               put("tah", "tahitian");
               put("tai", "tai_languages");
               put("tam", "tamil");
               put("tat", "tatar");
               put("tel", "telugu");
               put("tem", "timne");
               put("ter", "tereno");
               put("tet", "tetum");
               put("tgk", "tajik");
               put("tgl", "tagalog");
               put("tha", "thai");
               put("tig", "tigre");
               put("tir", "tigrinya");
               put("tiv", "tiv");
               put("tkl", "tokelau");
               put("tlh", "klingon");
               put("tli", "tlingit");
               put("tmh", "tamashek");
               put("tog", "tonga");
               put("ton", "tonga_nyasa");
               put("tpi", "tok_pisin");
               put("tsi", "tsimshian");
               put("tsn", "setswana");
               put("tso", "tsonga");
               put("tuk", "turkmen");
               put("tum", "tumbuka");
               put("tup", "tupi_languages");
               put("tur", "turkish");
               put("tut", "altaic_languages");
               put("tvl", "tuvalu");
               put("twi", "twi");
               put("tyv", "tuvinian");
               put("udm", "udmurt");
               put("uga", "ugaritic");
               put("uig", "uighur");
               put("ukr", "ukrainian");
               put("umb", "umbundu");
               put("urd", "urdu");
               put("uzb", "uzbek");
               put("vai", "vai");
               put("ven", "venda");
               put("vie", "vietnamese");
               put("vol", "volapuk");
               put("vot", "votic");
               put("wak", "wakashan_languages");
               put("wal", "walamo");
               put("war", "waray");
               put("was", "washo");
               put("wen", "sorbian_languages");
               put("wln", "walloon");
               put("wol", "wolof");
               put("xal", "kalmyk");
               put("xho", "xhosa");
               put("yao", "yao");
               put("yap", "yapese");
               put("yid", "yiddish");
               put("yor", "yoruba");
               put("ypk", "yupik_languages");
               put("zap", "zapotec");
               put("zbl", "bliss");
               put("zen", "zenaga");
               put("zha", "zhuang");
               put("zho", "chinese");
               // Traditional Chinese does not have an ISO 639-2 three-letter language code because it is not a different spoken language,
               // it is a different script language.  Using ZH-HANT IETF language tag.
               put("ZH-HANT", "traditional_chinese");
               put("znd", "zande_languages");
               put("zul", "zulu");
               put("zun", "zuni");
               put("zza", "zaza");
            }
         });

   public static final Map<String, String> LANGUAGE_CORRECTION =
      new HashMap<String, String>() {
         {
            put("portugese", "portuguese");
            put("pt", "portuguese");
            put("dutch; flemish", "dutch");
            put("nl", "dutch");
            put("spanish; castilian", "spanish");
            put("fr", "french");
            put("alsatian", "swiss_german");
            put("eo", "esperanto");
            put("catalan; valencian", "catalan");
         }
      };
      
   public static final Map<String, String> MARKETPLACE_CURRENCY_CODES = 
      new HashMap<String, String>() {
         {
            put("US", "USD");
            put("UK", "GBP");
            put("DE", "EUR");
            put("FR", "EUR");
            put("ES", "EUR");
            put("IN", "INR");
            put("IT", "EUR");
            put("JP", "JPY");
            put("CA", "CAD");
            put("CN", "CNY");
            put("BR", "BRL");
            put("MX", "MXN");
            put("AU", "AUD");
            put("RU", "RUB");
            put("NL", "EUR");
         }
      };
      
   // Most popular non-integer values in SCI1NA
   public static final Map<String, Integer> SERIES_VOLUME_DB_MAPPINGS = 
      new HashMap<String, Integer>() {
         {
            put("ONE", 1);
            put("TWO", 2);
            put("THREE", 3);
            put("FOUR", 4);
            put("FIVE", 5);
            put("SIX", 6);
            put("SEVEN", 7);
            put("EIGHT", 8);
            put("NINE", 9);
            put("TEN", 10);
            put("ELEVEN", 11);
            put("TWELVE", 12);
            put("THIRTEEN", 13);
            put("FOURTEEN", 14);
            put("FIFTEEN", 15);
            put("I", 1);
            put("II", 2);
            put("III", 3);
            put("IV", 4);
            put("V", 5);
            put("VI", 6);
            put("VII", 7);
            put("VIII", 8);
            put("IX", 9);
            put("X", 10);
            put("FIRST", 1);
            put("SECOND", 2);
            put("VOLUME 1", 1);
            put("VOLUME ONE", 1);
            put("VOLUME TWO", 2);
            put("VOLUME I", 1);
            put("VOLUME II", 2);
            put("VOL 1", 1);
            put("VOL.1", 1);
            put("VOL. 1", 1);
            put("VOL. 2", 2);
            put("VOL. 3", 3);
            put("#1", 1);
            put("PART 1", 1);
            put("PART 2", 2);
            put("PART ONE", 1);
            put("1.0", 1);
            put("2.0", 2);
            put("I.", 1);
            put("II.", 2);
            put("BOOK I", 1);
            put("BOOK #1", 1);
            put("BOOK ONE", 1);
            put("BOOK TWO", 2);
            put("BOOK THREE", 3);
            put("BOOK FOUR", 4);
            put("BOOK 1", 1);
            put("BOOK 2", 2);
            put("BOOK 3", 3);
            put("BOOK 4", 4);
            put("VOLUME 2", 2);
            put("VOLUME 3", 3);
            put("VOLUME 4", 4);
            put("VOLUME 5", 5);
            put("N/A", null);
            put("NONE", null);
            put("-", null);
            put("null", null);
         }
   };

   //NOTE: live_unpublished has no consistent publishingStatus.
   static final HashMap<String, PublishingStatus> DIGITAL_ITEM_STATUS_MAP = new HashMap<String, PublishingStatus>() {{
      put("draft", PublishingStatus.DRAFT);
      put("draft_review", PublishingStatus.PENDING_APPROVAL);
      put("draft_publishing", PublishingStatus.PUBLISHING);
      put("draft_blocked", PublishingStatus.BLOCKED);
      put("live", PublishingStatus.LIVE);
      put("live_with_changes", PublishingStatus.DRAFT);
      put("live_review", PublishingStatus.PENDING_APPROVAL);
      put("live_publishing", PublishingStatus.PUBLISHING);
      put("live_blocked", PublishingStatus.BLOCKED);
      //This state is ambiguous though it 'should' be DRAFT - /DigitalOpenPublishingService/src/amazon/dtp/dops/controllers/helpers/BookStateTransition.java
      put("live_unpublished", PublishingStatus.DRAFT);
      put("deleted", PublishingStatus.TRASHED);
   }};
   // CHECKSTYLE:UNSUPPRESS:MagicNumber
   
   static final HashMap<String, LiveItemStatus> DIGITAL_ITEM_LIVE_STATUS_MAP = new HashMap<String, LiveItemStatus>() {{
      //OFFER_FOUND will no longer be a valid state in DBC, and items in that state will instead be returned as PUBLISHED
      put("draft", LiveItemStatus.INACTIVE);
      put("draft_review", LiveItemStatus.INACTIVE);
      put("draft_publishing", LiveItemStatus.INACTIVE);
      put("draft_blocked", LiveItemStatus.INACTIVE);
      put("live", LiveItemStatus.PUBLISHED);
      put("live_with_changes", LiveItemStatus.PUBLISHED);
      put("live_review", LiveItemStatus.PUBLISHED);
      put("live_publishing", LiveItemStatus.PUBLISHING);
      put("live_blocked", LiveItemStatus.INACTIVE);
      put("live_unpublished", LiveItemStatus.UNPUBLISHED);
      put("deleted", LiveItemStatus.UNPUBLISHED);
   }};

   public static final Set<String> VAT_MARKETPLACES = 
      Stream.of("UK", "DE", "FR", "ES", "IT", "NL", "JP", "IN", "AU").collect(Collectors.toSet());
   
   public static final Integer MATCHBOOK_TO_CENTS_FACTOR = 100;

   private static final HashMap<String, String> DBC_TO_DOPS_REJECTION_CODE_MAP = new HashMap<String, String>() {
      {
         put("reflow_to_print_replica", "ReflowToPrintReplica");
         put("print_replica_to_reflow", "PrintReplicaToReflow");
         put("multimedia_book", "MultimediaBookNotSupported");
      }
   };
   
   public static final Map<String, String> CORRECTED_BISAC_CODES = new HashMap<String, String>() {
      {
         put("ARCHITECTURE > History > General", "ARC005000");
         put("ARCHITECTURE > Historic Preservation > General", "ARC014000");
         put("NATURE > Animals > General", "NAT001000");
         put("COMPUTERS > Enterprise Applications > General", "COM005000");
         put("SCIENCE > General", "SCI000000");
         put("POETRY > American > General", "POE005010");
         put("RELIGION > Hinduism > General", "REL032000");
         put("TRAVEL > Africa > General", "TRV002000");
         put("HISTORY > United States > General", "HIS036000");
         put("HISTORY > Caribbean & West Indies > General", "HIS041000");
         put("ART > Collections, Catalogs, Exhibitions > General", "ART006000");
         put("LANGUAGE ARTS & DISCIPLINES > Linguistics > General", "LAN009000");
         put("TRAVEL > South America > General", "TRV024000");
         put("RELIGION > General", "REL000000");
         put("PHOTOGRAPHY > Collections, Catalogs, Exhibitions > General", "PHO004000");
         put("LANGUAGE ARTS & DISCIPLINES > Library & Information Science > General", "LAN025000");
         put("RELIGION > Christianity > General", "REL070000");
      }
   };
   
   //Some JP titles have these categories selected with is_adult_content true, which is not allowed in DBC
   public static final Set<String> JUVENILE_BISAC_CODES = Stream.of("JUV000000","JUV001000","JUV001010","JUV005000",
      "JUV008000","JUV008010","JUV010000","JUV012030","JUV014000","JUV015020",
      "JUV018000","JUV019000","JUV021000","JUV026000","JUV027000","JUV028000",
      "JUV036000","JUV037000","JUV038000","JUV039090","JUV039140","JUV039190",
      "JUV040000","JUV047000","JUV051000","JUV052000","JUV053000","JUV054000",
      "JUV058000","JUV060000","JNF023000","JNF041000","JNF053020","JUV022030",
      "JNF009000","JNF040000","JNF028000","JNF028020","JNF000000","JUV023000",
      "JUV033020","JNF056000","JNF047000","JNF012010","JUV016080","JUV002180",
      "JUV039180","JUV056000","JUV039210","JNF014000","JUV016010","JUV061000",
      "JNF012030","JUV008020","JUV039010","JNF001000","JUV032070","JNF008000",
      "JNF024090","JUV049000","JNF062000","JUV013020","JUV013070","JNF002000",
      "JNF016000","JNF011000","JUV024000","JNF053100","JUV001020","JUV039050",
      "JNF030000","JNF051110","JNF017000","JUV007000","JUV002070","JNF060000",
      "JNF003060","JNF053200","JUV016000","JNF053010","JNF010000","JNF007120",
      "JNF059000","JNF034000","JNF021000","JNF024020").collect(Collectors.toSet());
   
   //Some titles have these categories selected with grade level set, which is not allowed in DBC
   public static final Set<String> EROTICA_BISAC_CODES = Stream.of("CGN004020","CGN004110",
      "FIC005000","PHO023030","FIC027010","FIC049030").collect(Collectors.toSet());
   
   public static final String FICTION_EROTICA_BISAC_CODE = "FIC005000";
   
   public static String correctLanguage(String language) {
      String lowercaseLanguage = Optional.ofNullable(language).map(String::toLowerCase).orElse(null);
      return LANGUAGE_CORRECTION.getOrDefault(lowercaseLanguage, lowercaseLanguage);
   }

   public static String lookupLanguageCode(String language) {
      return LANGUAGE_CODES.inverse().get(correctLanguage(language));
   }

   public static String lookupLanguage(String languageCode) {
      return LANGUAGE_CODES.get(languageCode);
   }
   
   public static final HashBiMap<ChargeMethodType, String> CHARGE_METHOD_MARKETPLACES = HashBiMap.create(new HashMap<ChargeMethodType, String>() {
      {
         put(ChargeMethodType.AU, "AU");
         put(ChargeMethodType.BR, "BR");
         put(ChargeMethodType.CA, "CA");
         put(ChargeMethodType.DE, "DE");
         put(ChargeMethodType.ES, "ES");
         put(ChargeMethodType.FR, "FR");
         put(ChargeMethodType.GLOBAL, "US");
         put(ChargeMethodType.IN, "IN");
         put(ChargeMethodType.IT, "IT");
         put(ChargeMethodType.JP, "JP");
         put(ChargeMethodType.MX, "MX");
         put(ChargeMethodType.NL, "NL");
         put(ChargeMethodType.UK, "UK");
      }
   });

   public static IonStruct dopsDataToIon(final DopsData data, final boolean isHeal) {
      try {
         DOPS_DATA_CORRECTED.set(null);
         return dopsDataToIon(data, isHeal, true);
      } finally {
         DOPS_DATA_CORRECTED.set(null);
      }
   }

   public static IonStruct dopsDataToIon(final DopsData data, final boolean isHeal, final boolean healLive) {
      DigitalItem digitalItem = data.getDigitalItem();
      LiveDigitalItem liveDigitalItem = digitalItem.getLiveDigitalItem();

      // isHeal is used to override attributes that cannot be unset from Potter in DBC
      // (when isHeal is true we need to override the unsettable DigitalItem attributes to null
      // in DBC in case they are set there)
      // TODO: isHeal needs to add nulls ONLY on a heal operation.  The alternative
      // is a change in Potter which will be a partial update so 'isHeal == false' needs to prevent
      // setting nulls.
      Book book = InterfaceTypeUtil.getKDPBook(digitalItem);
      IonStruct baseDigitalBookIon = ION_SYSTEM.newEmptyStruct();
      addString(baseDigitalBookIon, ITEM_SET_ID, digitalItem.getItemSetID());
      addString(baseDigitalBookIon, ISBN, book.getIsbn());
      if (!CollectionUtils.isEmpty(digitalItem.getCurrentOffers())) {
         addString(baseDigitalBookIon, ASIN,
            InterfaceTypeUtil.getAsinFromDigitalItem(digitalItem));
      } else if (isHeal) {
         addString(baseDigitalBookIon, ASIN, null);
      }

      addMatchbookIon(baseDigitalBookIon, digitalItem);

      IonStruct selectStatus = getProgramDetailIon(digitalItem.getProgramDetail(), isHeal, data);
      if (!selectStatus.isEmpty()){
         baseDigitalBookIon.put(SELECT_STATUS, selectStatus);
      } else if (isHeal) {
         baseDigitalBookIon.put(SELECT_STATUS, removeSentinel());
      }

      if (data.getFreePromotions() != null) {
         baseDigitalBookIon.put(FREE_PROMOTIONS, data.getFreePromotions());
      } else if (isHeal){
         baseDigitalBookIon.put(FREE_PROMOTIONS, removeSentinel());
      }
      
      Integer remainingPushesAllowed = digitalItem.getPreorder() == null ? null : digitalItem.getPreorder().getRemainingPushesAllowed(); 
      addInt(baseDigitalBookIon, PREORDER_REMAINING_PUSHES_ALLOWED, remainingPushesAllowed);

      addFeaturesIon(baseDigitalBookIon, digitalItem);
      
      IonStruct scopedIon = buildScopedIon(data, isHeal);
      if(!scopedIon.isEmpty()) {
         IonStruct draftScopedIon = scopedIon.clone();

         draftScopedIon.put(FILE_PROCESSING, data.getDraftFileProcessing());
         draftScopedIon.put(S3_PUBLISHER_COVER, data.getS3PublisherCover());
         draftScopedIon.put(S3_PUBLISHER_INTERIOR, data.getS3PublisherInterior());
         draftScopedIon.put(LAST_CHANGED_DATE).newTimestamp(data.getDraftLastChangedDate());
         baseDigitalBookIon.put(DRAFT_SCOPE, draftScopedIon);
         if(isHeal && healLive) {
            //TODO: Maybe do this for a blocked title's pre-blocked publishing status? Haven't decided.
            IonStruct scopedIonWithRequiredFields = buildScopedIonWithRequiredFields(scopedIon.clone());

            if(digitalItem.getPublishingStatus() == PublishingStatus.PENDING_APPROVAL) {
               IonStruct scopedIonWithReviewId = scopedIonWithRequiredFields.clone();
               scopedIonWithReviewId.put("review_id", ION_SYSTEM.newInt(0));
               scopedIonWithReviewId.put(FILE_PROCESSING, data.getReviewFileProcessing());
               baseDigitalBookIon.put(REVIEW_SCOPE, scopedIonWithReviewId);
            }
            else if(digitalItem.getPublishingStatus() == PublishingStatus.PUBLISHING ||
                  digitalItem.getPublishingStatus() == PublishingStatus.LIVE) {
               IonStruct scopedIonWithReviewId = scopedIonWithRequiredFields.clone();
               scopedIonWithReviewId.put("review_id", ION_SYSTEM.newInt(0));
               scopedIonWithReviewId.put(FILE_PROCESSING, data.getReviewFileProcessing());
               baseDigitalBookIon.put(REVIEW_SCOPE, scopedIonWithReviewId);
               IonStruct scopedIonWithReviewIdAndPublicationDate = scopedIonWithReviewId.clone();
               if (anyNull(scopedIonWithReviewIdAndPublicationDate.get(PUBLICATION_DATE))) {
                  Date preorderReleaseDate = getPreorderReleaseDate(digitalItem, false);
                  Date publicationDate = preorderReleaseDate == null ? Date.from(digitalItem.getPublicationDate().toInstant().plus(Duration.ofDays(1l))) : preorderReleaseDate;
                  addDate(scopedIonWithReviewIdAndPublicationDate, PUBLICATION_DATE, publicationDate);
               }
               baseDigitalBookIon.put(LIVE_SCOPE, scopedIonWithReviewIdAndPublicationDate);
            }
         }
      }
      
      if(isHeal && healLive && (baseDigitalBookIon.get(REVIEW_SCOPE) == null || baseDigitalBookIon.get(LIVE_SCOPE) == null || Boolean.TRUE.equals(DOPS_DATA_CORRECTED.get()))) {
         IonStruct payload = ION_SYSTEM.newEmptyStruct();
         payload.put("item_set_id", ION_SYSTEM.newString(digitalItem.getItemSetID()));
         IonStruct dbcLiveUpdate = DIGITAL_BOOK_RESOURCE.customAction(DigitalBookResource.CustomAction.DPPToDBC, payload);
         if(!IonValueUtils.anyNull(dbcLiveUpdate)) {
            IonStruct dbcLive = (IonStruct)dbcLiveUpdate.get(LIVE_SCOPE);
            if(baseDigitalBookIon.get(REVIEW_SCOPE) == null) {
               IonStruct reviewScope = dbcLive.clone();
               reviewScope.put(PUBLICATION_DATE).newNullTimestamp();
               baseDigitalBookIon.put(REVIEW_SCOPE, reviewScope);
            }
            if(baseDigitalBookIon.get(LIVE_SCOPE) == null) {
               baseDigitalBookIon.put(LIVE_SCOPE, dbcLive.clone());
            }
            //log corrected titles in the transform that are invalid in DPP
            if (Boolean.TRUE.equals(DOPS_DATA_CORRECTED.get())) {
               DigitalItem dppItem = new DigitalItem();
               TypeSafeIonStruct typeSafeDBCLive = IonStructs.newTypeSafeStruct(dbcLive);
               addGradeLevelRange(typeSafeDBCLive, dppItem);
               addReadingInterestAge(typeSafeDBCLive, dppItem);
               addCategories(typeSafeDBCLive, dppItem);
               boolean isAdultContentDPP = ((!anyNull(dbcLive.get(IS_ADULT_CONTENT)) && ((IonBool)dbcLive.get(IS_ADULT_CONTENT)).booleanValue()) || hasEroticaBISAC(dppItem));
               if (!isAdultContentDPP ||
                   ((dppItem.isSetMinAge() && dppItem.getMinAge() != 18) ||
                    (dppItem.isSetMaxAge() && dppItem.getMaxAge() != 18) ||
                    !StringUtils.isEmpty(dppItem.getMinGradeLevel()) ||
                    !StringUtils.isEmpty(dppItem.getMaxGradeLevel()))) {
                  logger.info("[STONES] item corrected in DBC has non-adult discrepancies in DPP. DPPToDBC: " + dbcLive);
               }
               ProfilerScope.addCounter("DBC.invalidDPPTitle", 1, "");
            } else {
               ProfilerScope.addCounter("DBC.invalidDPPTitle", 0, "");
            }
         }
      }
      
      //Re-compute amazon_channel_prices_verified for titles with invalid prices
      if (!anyNull(baseDigitalBookIon.get(REVIEW_SCOPE))) {
         IonStruct review = (IonStruct) baseDigitalBookIon.get(REVIEW_SCOPE);
         review.put("amazon_channel_prices_verified").newBool(true);
      }
      if (!anyNull(baseDigitalBookIon.get(LIVE_SCOPE))) {
         IonStruct live = (IonStruct) baseDigitalBookIon.get(LIVE_SCOPE);
         live.put("amazon_channel_prices_verified").newBool(true);
      }
      
      /***** State *****/
      IonStruct state = ION_SYSTEM.newEmptyStruct();
      if(digitalItem.getPreorder() != null) {
         Preorder preorder = digitalItem.getPreorder();
         Timestamp preorderLiveDate = null;
         if (liveDigitalItem != null && liveDigitalItem.getLivePreorder() != null &&
            liveDigitalItem.getLivePreorder().getReleaseDate() != null) {
            preorderLiveDate = Timestamp.forDateZ(liveDigitalItem.getLivePreorder().getReleaseDate());
         }
         state.put(STATE_PREORDER_CANCELLED_DATE,
            ION_SYSTEM.newTimestamp(Timestamp.forDateZ(preorder.getCancellationDate())));
         state.put(STATE_PREORDER_CANCELLED_REASON, ION_SYSTEM.newString(preorder.getCancellationReason()));
         if (isHeal) {
            state.put(STATE_PREORDER_LIVE_DATE, ION_SYSTEM.newTimestamp(preorderLiveDate));
         }
      } else if (isHeal) {
         state.put(STATE_PREORDER_CANCELLED_DATE).newNullTimestamp();
         state.put(STATE_PREORDER_CANCELLED_REASON).newNullString();
         state.put(STATE_PREORDER_LIVE_DATE).newNullTimestamp();
      }
      
      if(digitalItem.getPublishingStatus() == PublishingStatus.DRAFT ||
         digitalItem.getPublishingStatus() == PublishingStatus.READY ||
         (!isHeal && data.getStateTransitionRequired() && digitalItem.getPublishingStatus() == PublishingStatus.LIVE)) {
         state.put(STATE_PUBLISHER_CHANGE_DATE, ION_SYSTEM.newTimestamp(Timestamp.nowZ()));
      }
      
      if (isHeal) {
         if(data.getDeletedDate() == null) {
            state.put(STATE_DELETED_DATE, ION_SYSTEM.newNullTimestamp());
         }
         else {
            state.put(STATE_DELETED_DATE, ION_SYSTEM.newTimestamp(data.getDeletedDate()));
         }
         if(data.getUnpublishedDate() != null) {
            state.put(STATE_UNPUBLISHED_DATE, ION_SYSTEM.newTimestamp(data.getUnpublishedDate()));
         }
         if((digitalItem.getPublishingStatus() == PublishingStatus.DRAFT || digitalItem.getPublishingStatus() == PublishingStatus.READY) && data.getLiveVerifiedDate() != null) {
            state.put(STATE_PUBLISHER_CHANGE_DATE, ION_SYSTEM.newTimestamp(data.getLiveVerifiedDate().addSecond(1)));
         }
         state.put(STATE_LIVE_VERIFIED_DATE, ION_SYSTEM.newTimestamp(data.getLiveVerifiedDate()));
         state.put(STATE_PUBLISHING_START_DATE, ION_SYSTEM.newTimestamp(data.getPublishingStartDate()));
         state.put(STATE_REVIEW_END_DATE, ION_SYSTEM.newTimestamp(data.getReviewEndDate()));
         state.put(STATE_REVIEW_START_DATE, ION_SYSTEM.newTimestamp(data.getReviewStartDate()));
         if (data.getBlockedReason() != null) {
            state.put(STATE_BLOCKED_REASONS).newEmptyList().add().newString(data.getBlockedReason());
         } else {
            state.put(STATE_BLOCKED_REASONS).newNullList();
         }
         if (isHeal) {
            state.put("blocked_reason", removeSentinel());
         }
      }
      
      if(!state.isEmpty()) {
         baseDigitalBookIon.put(STATE, state);
      }
      
      addMetricsIon(baseDigitalBookIon, digitalItem);
      
      if(isHeal) {
         IonStruct kuBorrowPrice = ION_SYSTEM.newEmptyStruct();
         if(data.getKuBorrowPrice() != null) {
            kuBorrowPrice.put(KU_BORROW_PRICE_PER_PAGE, ION_SYSTEM.newDecimal(data.getKuBorrowPrice()));
         }
         if(data.getKuBorrowPriceSource() != null) {
            kuBorrowPrice.put(KU_BORROW_PRICE_SOURCE, ION_SYSTEM.newString(data.getKuBorrowPriceSource().value));
         }
         
         if(!kuBorrowPrice.isEmpty()) {
            baseDigitalBookIon.put(KU_BORROW_PRICE, kuBorrowPrice);
         }
      }
      
      //Propagate KCD Price Change Dates.  This needs to happen after we re-built live from either DigitalItem or DPP.
      IonStruct live = (IonStruct)baseDigitalBookIon.get(LIVE_SCOPE);
      if(liveDigitalItem != null && !anyNull(live) && liveDigitalItem.getLiveChargeMethods() != null) {
         IonStruct liveAmazonChannel = (IonStruct)live.get(AMAZON_CHANNEL);
         for(LiveChargeMethod m : (List<LiveChargeMethod>)liveDigitalItem.getLiveChargeMethods()) {
            Date kcdPriceChangeDate = m.getKcdPriceChangedDate();
            String marketplace = CHARGE_METHOD_MARKETPLACES.get(m.getType());
            if(kcdPriceChangeDate != null && marketplace != null && liveAmazonChannel.containsKey(marketplace)) {
               IonStruct marketplacePrice = (IonStruct)liveAmazonChannel.get(marketplace);
               marketplacePrice.put(LAST_CHANGED_DATE, ION_SYSTEM.newTimestamp(Timestamp.forDateZ(kcdPriceChangeDate)));
            }
         }
      }
      
      return baseDigitalBookIon;
   }

   private static IonStruct buildScopedIon(final DopsData data, final boolean isHeal) {
      DigitalItem digitalItem = data.getDigitalItem();
      Book digitalBook = (Book)digitalItem.getDigitalItemType();

      IonStruct scope = ION_SYSTEM.newEmptyStruct();
      addBool(scope, IS_DRM, digitalBook.getIsDRM());
      addString(scope, PISBN, null);
      addString(scope, PROVENANCE, digitalItem.getProvenance());
      if (digitalItem.getConversionType() != null) {
         addString(scope, INTERIOR_TYPE, digitalItem.getConversionType().getValue());
      }
      // TODO file processing attributes
      addString(scope, DRAFT_COVER_CREATOR_DESIGN_ID,
            digitalBook.getCoverCreatorDraftId());
      addString(scope, SAVED_COVER_CREATOR_DESIGN_ID,
            digitalBook.getCoverCreatorSavedId());
      addCoverChoiceAttributes(scope, digitalItem.getCoverChoice());
      addBool(scope, IS_BOOK_LENDING, digitalItem.isSetLendingPlan() ? LendingPlan.BASIC.equals(digitalItem.getLendingPlan()) : null);
      //Converting royalty_plan_id to decimal rate in DBC, deprecating royalty_plan string in DBC
      if (digitalItem.isSetRoyaltyPlanId()) {
         Double royaltyRate = ROYALTY_PLAN_TO_RATE_MAPPING.get(digitalItem.getRoyaltyPlanId());
         addDecimal(scope, ROYALTY_RATE, royaltyRate);
         scope.put(ROYALTY_PLAN, ION_SYSTEM.newNullString());
      } else {
         scope.put(ROYALTY_PLAN, ION_SYSTEM.newNullString());
         scope.put(ROYALTY_RATE, ION_SYSTEM.newNullDecimal());
      }
      // Not using edition_number in DBC due to the type mismatch with DOPS
      addString(scope, EDITION, digitalBook.getEdition());
      // TODO edition_romanized is not used in Potter since edition is validated as integer
      //      Not porting over this attribute over to DBC for now
      //      addString(scope, "edition_romanized",
      //         digitalBook.getEditionRomanized());
      
      if (!CollectionUtils.isEmpty(digitalItem.getCurrentOffers())) {
         addAmazonChannelIon(scope, digitalItem);
      } else if (isHeal) {
         scope.put(AMAZON_CHANNEL, ION_SYSTEM.newNullStruct());
      }
      
      addString(scope, TITLE, digitalItem.getTitle());
      addString(scope, TITLE_PRONUNCIATION, digitalItem.getTitlePronunciation());
      addString(scope, TITLE_ROMANIZED, digitalItem.getTitleRomanized());
      addString(scope, SUBTITLE, digitalBook.getSubtitle());
      addString(scope, SUBTITLE_PRONUNCIATION,
         digitalBook.getSubtitlePronunciation());
      addString(scope, SUBTITLE_ROMANIZED,
         digitalBook.getSubtitleRomanized());
      addString(scope, LANGUAGE, lookupLanguageCode(digitalItem.getLanguage()));
      addString(scope, SERIES_TITLE, digitalBook.getSeriesTitle());
      addString(scope, SERIES_TITLE_PRONUNCIATION,
         digitalBook.getSeriesTitlePronunciation());
      addString(scope, SERIES_TITLE_ROMANIZED,
         digitalBook.getSeriesTitleRomanized());
      addSeriesNumber(scope, digitalBook.getSeriesVolume());
      addString(scope, PUBLISHER, digitalBook.getPublisher());
      addString(scope, PUBLISHER_ROMANIZED, digitalBook.getPublisherRomanized());
      addString(scope, IMPRINT, digitalBook.getImprint());
      addString(scope, PUBLISHER_LABEL, digitalBook.getPublisherLabel());
      addString(scope, PUBLISHER_LABEL_PRONUNCIATION,
         digitalBook.getPublisherLabelPronunciation());
      addString(scope, PUBLISHER_LABEL_ROMANIZED,
         digitalBook.getPublisherLabelRomanized());
      if (!CollectionUtils.isEmpty(digitalItem.getContributors())) {
         addContributorsIon(scope, digitalItem.getContributors());
      } else {
         scope.put(CONTRIBUTORS, ION_SYSTEM.newNullList());
      }
      addInternalPagesIon(scope, digitalItem.getInternalPages());
      addString(scope, DESCRIPTION, StringUtils.strip(digitalItem.getProductDescription(), " "));
      addBool(scope, IS_PUBLIC_DOMAIN,
            digitalBook.isSetPublishingRights() ? PublishingRightsStatus.PUBLIC_DOMAIN.equals(digitalBook.getPublishingRights()) : null);
      addStringList(scope, KEYWORDS, (List<String>) digitalItem.getKeywords());

      if (digitalItem.isSetAdultContentType() || hasEroticaBISAC(digitalItem)) {
         //Correct titles with erotica categories that are not is adult content in DOPS. Currently DOPS always sends is_adult false to DPP except for adult content JP titles
         addBool(scope, IS_ADULT_CONTENT, AdultContentType.APPLICABLE == digitalItem.getAdultContentType() || hasEroticaBISAC(digitalItem));
      } else {
         scope.put(IS_ADULT_CONTENT, ION_SYSTEM.newNullBool());
      }
      if (!CollectionUtils.isEmpty(digitalItem.getCategories())) {
         addCategoriesIon(scope, digitalItem.getCategories(), isHeal);
      } else {
         scope.put(CATEGORIES, ION_SYSTEM.newNullList());
         scope.put(THESAURUS_SUBJECT_KEYWORDS, ION_SYSTEM.newNullList());
      }
      addString(scope, PAGE_TURN_DIRECTION, digitalBook.getPageTurnDirection());
      Date preorderReleaseDate = getPreorderReleaseDate(digitalItem, true);
      // publication_date is set on draft only for preorder titles
      addDate(scope, PUBLICATION_DATE, preorderReleaseDate);
      addReadingInterestAgeIon(scope, digitalItem, isHeal);
      addGradeLevelIon(scope, digitalItem, isHeal);
      scope.put(TERRITORY_RIGHTS, getSalesTerritoriesIon(digitalItem.getPublishingStatus(), (Set<SalesTerritory>) digitalItem.getSalesTerritories()));

      //KW
      if (digitalBook.getFanFictionBook() != null) {
         FanFictionBook fanFictionBook = digitalBook.getFanFictionBook();
         addString(scope, KW_CONTENT_LENGTH, fanFictionBook.getContentLength());
         addString(scope, KW_UNIVERSE_ID, fanFictionBook.getUniverseId());
         addString(scope, KW_UNIVERSE_NAME, fanFictionBook.getUniverseName());
         addInt(scope, KW_WORD_COUNT, digitalItem.getWordCount());
      }
      
      return scope;
   }
   
   private static IonStruct buildScopedIonWithRequiredFields(IonStruct scopedIon) {
      if(IonValueUtils.anyNull(scopedIon.get(IS_PUBLIC_DOMAIN))) {
         addBool(scopedIon, IS_PUBLIC_DOMAIN, false);
      }
      if(IonValueUtils.anyNull(scopedIon.get(IS_BOOK_LENDING))) {
         addBool(scopedIon, IS_BOOK_LENDING, false);
      }
      if(IonValueUtils.anyNull(scopedIon.get(HOME_MARKETPLACE))) {
         addString(scopedIon, HOME_MARKETPLACE, "US");
      }
      
      return scopedIon;
   }

   public static Integer parseSeriesNumber(String seriesVolume) {
      if (seriesVolume != null) {
         try {
            return Integer.parseInt(seriesVolume);
         } catch (NumberFormatException e) {
            return SERIES_VOLUME_DB_MAPPINGS.get(seriesVolume.toUpperCase().trim());
         }
      } else {
         return null;
      }
   }
   
   private static void addSeriesNumber(final IonStruct scope, final String seriesVolume) {
      addString(scope, SERIES_VOLUME, seriesVolume);
      if (StringUtils.isEmpty(seriesVolume)) {
         scope.put(SERIES_NUMBER, ION_SYSTEM.newNullInt());
      } else {
         Integer converted = parseSeriesNumber(seriesVolume);
         if (converted != null) {
            scope.put(SERIES_NUMBER, ION_SYSTEM.newInt(converted));
         } else {
            scope.put(SERIES_NUMBER, ION_SYSTEM.newNullInt());
         }
      }
   }

   protected static IonStruct getProgramDetailIon(final ProgramDetail programDetail, final boolean isHeal, DopsData data) {
      final IonStruct selectStatus = ION_SYSTEM.newEmptyStruct();
      boolean hasSelectData = programDetail != null && programDetail.hasLatestExclusiveProgram();
      boolean hasEligibilityData = data.getEligibilityOverrideDate() != null  || data.getEligibilityOverrideReason() != null;
      if (!hasSelectData && !hasEligibilityData) {
         return selectStatus;
      }

      if (hasSelectData) {
         final ExclusiveProgram latestProgram = programDetail.getLatestExclusiveProgram();

         Date enrolledDate = null;
         Date firstActiveDate = null;
         Date lastRenewalDate = null;
         List<ExclusiveProgram> allPrograms = (List<ExclusiveProgram>) programDetail.getAllExclusivePrograms();
         ExclusiveProgram lastOptOut = allPrograms.stream()
               .filter(p -> ExclusiveProgramStatus.OPTED_OUT == p.getProgramStatus())
               .max(Comparator.comparing(ExclusiveProgram::getActionDate))
               .orElse(null);

         if (ExclusiveProgramStatus.NOT_STARTED.equals(latestProgram.getProgramStatus())) {
            if (lastOptOut != null) {
               addDate(selectStatus,
                     SELECT_STATUS_NOT_STARTED_DATE,
                     lastOptOut.getActionDate());            
            }
            if (latestProgram.hasEnrollmentDate()) {
               addDate(selectStatus,
                     SELECT_STATUS_NOT_STARTED_DATE,
                     latestProgram.getActionDate());
            }
            else {
               return selectStatus;
            }
         }
         else if (isHeal) {
            selectStatus.add(SELECT_STATUS_NOT_STARTED_DATE,
                  ION_SYSTEM.newNullTimestamp());
         }

         ExclusiveProgram currentContiguousPeriodOldestProgram = findOldestProgramFromCurrentContiguousPeriod(allPrograms);
         if (currentContiguousPeriodOldestProgram != null) {
            enrolledDate = currentContiguousPeriodOldestProgram.getEnrollmentDate();
            firstActiveDate = currentContiguousPeriodOldestProgram.getStartDate();

            if (latestProgram.getExclusiveProgramId() != currentContiguousPeriodOldestProgram.getExclusiveProgramId()) {
               lastRenewalDate = latestProgram.getStartDate();
            }
         }

         if (enrolledDate != null || isHeal) {
            Date startDate = latestProgram.getStartDate();
            Date persistDate = startDate != null && enrolledDate != null && enrolledDate.after(startDate) ? startDate : enrolledDate;
            addDate(selectStatus,
                  SELECT_STATUS_ENROLLED_DATE,
                  persistDate);
         }

         if (firstActiveDate != null || isHeal) {
            addDate(selectStatus,
                  SELECT_STATUS_FIRST_ACTIVE_DATE,
                  firstActiveDate);
            addDate(selectStatus,
                  SELECT_STATUS_LAST_ACTIVE_DATE,
                  firstActiveDate);
         }

         if ((latestProgram.hasEndDate() && !latestProgram.isIsRenewable())) {
            addDate(selectStatus,
                  SELECT_STATUS_CURRENT_END_DATE,
                  latestProgram.getEndDate());
         }
         else if (isHeal) {
            selectStatus.add(SELECT_STATUS_CURRENT_END_DATE, ION_SYSTEM.newNullTimestamp());
         }

         if (ExclusiveProgramStatus.OPTED_OUT.equals(latestProgram.getProgramStatus()) ||
             ExclusiveProgramStatus.CANCELLED.equals(latestProgram.getProgramStatus())) {
            addDate(selectStatus,
                  SELECT_STATUS_OPT_OUT_DATE,
                  latestProgram.getActionDate());
         } else if (lastOptOut != null) {
            addDate(selectStatus,
                  SELECT_STATUS_OPT_OUT_DATE,
                  lastOptOut.getActionDate());
         } else if (isHeal) {
            selectStatus.add(SELECT_STATUS_OPT_OUT_DATE, ION_SYSTEM.newNullTimestamp());
         }

         if (ExclusiveProgramStatus.UNPUBLISHED.equals(latestProgram.getProgramStatus())) {
            addDate(selectStatus,
                  SELECT_STATUS_UNPUBLISHED_DATE,
                  latestProgram.getActionDate());
         } else if (isHeal) {
            selectStatus.add(SELECT_STATUS_UNPUBLISHED_DATE, ION_SYSTEM.newNullTimestamp());
         }

         if (ExclusiveProgramStatus.TERMINATED.equals(latestProgram.getProgramStatus())) {
            addDate(selectStatus,
                  SELECT_STATUS_TERMINATED_DATE,
                  latestProgram.getActionDate());
         } else if (isHeal) {
            selectStatus.add(SELECT_STATUS_TERMINATED_DATE, ION_SYSTEM.newNullTimestamp());
         }

         if (latestProgram.hasIsRenewable()) {
            selectStatus.add(SELECT_STATUS_AUTO_RENEW,
                  ION_SYSTEM.newBool(latestProgram.getIsRenewable()));
         } else if (isHeal) {
            selectStatus.add(SELECT_STATUS_AUTO_RENEW, ION_SYSTEM.newNullBool());
         }

         if (lastRenewalDate != null) {
            addDate(selectStatus, SELECT_STATUS_LAST_RENEWAL_DATE, lastRenewalDate);
         }
      }      

      if (data.getEligibilityOverrideDate() != null) {
         selectStatus.add(SELECT_STATUS_ELIGIBILITY_OVERRIDE_DATE,
               ION_SYSTEM.newTimestamp(data.getEligibilityOverrideDate()));
      } else if (isHeal) {
         selectStatus.add(SELECT_STATUS_ELIGIBILITY_OVERRIDE_DATE, ION_SYSTEM.newNullTimestamp());
      }
      
      if (data.getEligibilityOverrideReason() != null) {
         selectStatus.add(SELECT_STATUS_ELIGIBILITY_OVERRIDE_REASON,
               ION_SYSTEM.newString(data.getEligibilityOverrideReason()));
      } else if (isHeal) {
         selectStatus.add(SELECT_STATUS_ELIGIBILITY_OVERRIDE_REASON, ION_SYSTEM.newNullString());
      }

      return selectStatus;
   }

   private static ExclusiveProgram findOldestProgram(List<ExclusiveProgram> programs) {
      return programs.get(programs.size() - 1);
   }
   
   public static ExclusiveProgram findOldestProgramFromCurrentContiguousPeriod(List<ExclusiveProgram> programs) {
      ExclusiveProgram prev = null;
      ExclusiveProgram ret = null;
      boolean checkPossibleOrphanedNotStartedProgram = false;
      for (ExclusiveProgram program : programs) {
         if (ret == null) {
            prev = program;
            ret = program;
            continue;
         }

         if (prev.getStartDate() != null && prev.getStartDate().equals(program.getStartDate())) {
            // Skip duplicate rows
            prev = program;
            continue;
         }
         
         if (ExclusiveProgramStatus.NOT_STARTED.equals(program.getProgramStatus())) {
            checkPossibleOrphanedNotStartedProgram = true;
            continue;
         }

         if (checkPossibleOrphanedNotStartedProgram) {
            checkPossibleOrphanedNotStartedProgram = false;
            if (prev.getStartDate() == null ||
                program.getProgramStatus() != ExclusiveProgramStatus.EXPIRED ||
                program.getStartDate() == null ||
                !program.hasIsRenewable() ||
                !program.isIsRenewable() ||
                !getNextStartDate(program.getStartDate()).equals(prev.getStartDate())) {
               break;
            }
         }

         if (ExclusiveProgramStatus.EXPIRED.equals(program.getProgramStatus()) &&
               program.hasIsRenewable() && program.isIsRenewable()) {
            prev = program;
            ret = program;
            continue;
         }
         break;
      }
      return ret;
   }

   private static void addCoverChoiceAttributes(final IonStruct scope, final String coverChoice) {
      if (!StringUtils.isEmpty(coverChoice)) {
         if ("upload".equals(coverChoice)) {
            addString(scope, PUBLISHER_COVER_CHOICE, "UPLOAD");
         } else if ("quick-cover".equals(coverChoice)) {
            addString(scope, PUBLISHER_COVER_CHOICE, "QUICK_COVER");
         } else {
            // For cover creator, coverChoice could be set to
            // cover-creator, no_image, gallery, or uploaded.
            // The last 3 options are set by TylerTitleWorkflow based on the publisher's choice
            // when using cover creator. There is a race condition between Potter and the workflow
            // that persists the coverChoice as either cover-creator or one of the 3 values based
            // on which save happens last. Potter only recognizes cover_creator or upload for the 
            // attribute, so the other 3 options are persisted to a different attribute in DBC
            addString(scope, PUBLISHER_COVER_CHOICE, "COVER_CREATOR");
            if (!"cover-creator".equals(coverChoice)) {
               addString(scope, COVER_CREATOR_CHOICE, coverChoice);
            }
         }
      } else {
         addString(scope, PUBLISHER_COVER_CHOICE, null);
         addString(scope, COVER_CREATOR_CHOICE, null);
      }
   }

   private static void addMatchbookIon(final IonStruct val, final DigitalItem digitalItem) {
      if (!CollectionUtils.isEmpty(digitalItem.getAutoLitListings()) && InterfaceTypeUtil.getMatchbookListing(digitalItem).isEnrolled()) {
         IonDecimal matchbookPrice = InterfaceTypeUtil.getMatchbookPriceUSD(digitalItem);
         IonStruct matchbookPriceIon = ION_SYSTEM.newEmptyStruct();
         IonStruct matchbookUS = ION_SYSTEM.newEmptyStruct();
         if (matchbookPrice != null) {
            matchbookPriceIon.put(MATCHBOOK_PRICE, matchbookPrice);
            matchbookUS.put(MATCHBOOK_MARKETPLACE, matchbookPriceIon);
            val.put(MATCHBOOK, matchbookUS);
         } else {
            matchbookUS.put(MATCHBOOK_MARKETPLACE, removeSentinel());
            val.put(MATCHBOOK, matchbookUS);
         }
      } else {
         val.put(MATCHBOOK, removeSentinel());
      }
   }

   @VisibleForTesting
   public static void addAmazonChannelIon(final IonStruct scope, final DigitalItem digitalItem) {
      DigitalOffer digitalOffer = InterfaceTypeUtil.getDigitalOffer(digitalItem);
      List<ChargeMethod> chargeMethods = digitalOffer.getChargeMethods();
      String baseMarketplace = null;
      if (!CollectionUtils.isEmpty(chargeMethods)) {
         IonStruct amazonChannelIon = ION_SYSTEM.newEmptyStruct();
         for (ChargeMethod chargeMethod : chargeMethods) {
            String marketplace = InterfaceTypeUtil.getMarketplaceFromChargeMethod(chargeMethod);
            addChargeMethodIon(amazonChannelIon, chargeMethod, marketplace);
            if (PriceType.BASE == chargeMethod.getPriceType()) {
               baseMarketplace = marketplace;
            }
         }
         scope.put(AMAZON_CHANNEL, amazonChannelIon);
         if (baseMarketplace == null) {
            baseMarketplace = chargeMethods.stream()
               .filter(chargeMethod -> ChargeMethodType.GLOBAL == chargeMethod.getType()
                  && chargeMethod.getPrice() > 0)
               .map(InterfaceTypeUtil::getMarketplaceFromChargeMethod)
               .findFirst()
               .orElse(null);
         }
      } else {
         scope.put(AMAZON_CHANNEL, ION_SYSTEM.newNullStruct());
      }
      
      String homeMarketplace = digitalItem.getHomeMarketplace();
      if(StringUtils.isEmpty(homeMarketplace)) {
         homeMarketplace = baseMarketplace;
      }
      if("IN1".equals(homeMarketplace)) {
         homeMarketplace = "US";
      }
      if("GB".equals(homeMarketplace)) {
         homeMarketplace = "UK";
      }
      addString(scope, HOME_MARKETPLACE, homeMarketplace);
   }

   private static void addChargeMethodIon(final IonStruct struct,
         final ChargeMethod chargeMethod, final String marketplace) {
      if (ChargeMethodType.IN1.equals(chargeMethod.getType()) || 
          ChargeMethodType.CN.equals(chargeMethod.getType())) {
         //IN1 and CN are no longer used but still exist for many DB items
         return;
      }
      IonStruct marketplaceIon = ION_SYSTEM.newEmptyStruct();
      double roundedPrice = Math.round(chargeMethod.getPrice() * ROUNDING_FACTOR) / ROUNDING_FACTOR;
      addDecimal(marketplaceIon, PRICE_VAT_EXCLUSIVE, roundedPrice);
      addDecimal(marketplaceIon, PRICE_VAT_INCLUSIVE, 
         VAT_MARKETPLACES.contains(marketplace) ?
            chargeMethod.getPriceWithTax() :
            roundedPrice);

      String currencyCode = MARKETPLACE_CURRENCY_CODES.get(marketplace);
      addString(marketplaceIon, CURRENCY_CODE, currencyCode);

      // PriceType is MANUAL, AUTOMATIC, or BASE
      // BASE and MANUAL are not converted because they are the home marketplace price
      // and user-specified override price per marketplace respectively
      addBool(marketplaceIon, CONVERTED, chargeMethod.getPriceType() == null ? chargeMethod.getIsConvertedPrice() : chargeMethod.getPriceType() == PriceType.AUTOMATIC);
      struct.put(marketplace, marketplaceIon);
   }

   private static IonStruct getSalesTerritoriesIon(final PublishingStatus publishingStatus, final Set<SalesTerritory> salesTerritories) {
      IonStruct struct = ION_SYSTEM.newEmptyStruct();
      //Some items still have AN as a sales territory even though it's not supported in KDP anymore
      Set<String> salesTerritoryCountryCodes = salesTerritories == null ? 
            new HashSet<>() :
            salesTerritories.stream().filter(t -> !"AN".equals(t.getCountryCode())).map(t -> t.getCountryCode()).collect(Collectors.toSet());
      
      boolean salesTerritoriesRequiredButMissing = (publishingStatus == PublishingStatus.PENDING_APPROVAL || publishingStatus == PublishingStatus.PUBLISHING || publishingStatus == PublishingStatus.LIVE) && (salesTerritories == null || salesTerritories.isEmpty());
      if(salesTerritoriesRequiredButMissing || CollectionUtils.isEqualCollection(SALES_TERRITORIES, salesTerritoryCountryCodes)) {
         struct.put("**", ION_SYSTEM.newBool(true));
      }
      else if(!salesTerritoryCountryCodes.isEmpty()) {
         for (String countryCode : SALES_TERRITORIES) {
            if (salesTerritoryCountryCodes.contains(countryCode)) {
               struct.put(countryCode, ION_SYSTEM.newBool(true));
            }
         }
      }
      return struct.isEmpty() ? ION_SYSTEM.newNullStruct() : struct;
   }
   
   private static void addReadingInterestAgeIon(
      final IonStruct scope, final DigitalItem digitalItem, final boolean isHeal) {
      if (digitalItem.isSetMinAge() || digitalItem.isSetMaxAge()) {
         // Fix invalid adult content reading interest ages
         if ((digitalItem.getAdultContentType() == AdultContentType.APPLICABLE || hasEroticaBISAC(digitalItem)) &&
             ((digitalItem.isSetMinAge() && digitalItem.getMinAge() != 18) ||
             (digitalItem.isSetMaxAge() && digitalItem.getMaxAge() != 18))) {
            IonStruct struct = ION_SYSTEM.newEmptyStruct();
            struct.put(READING_INTEREST_AGE_MIN, ION_SYSTEM.newInt(18));
            struct.put(READING_INTEREST_AGE_MAX, ION_SYSTEM.newInt(18));
            scope.put(READING_INTEREST_AGE, struct);
            DOPS_DATA_CORRECTED.set(true);
            ProfilerScope.addCounter("DBC.correctedReadingInterest", 1, "");
         } else {
            IonStruct struct = ION_SYSTEM.newEmptyStruct();
            if (digitalItem.isSetMinAge()) {
               struct.put(READING_INTEREST_AGE_MIN, ION_SYSTEM.newInt(digitalItem.getMinAge()));
            } else {
               struct.put(READING_INTEREST_AGE_MIN, ION_SYSTEM.newNullInt());
            }
            if (digitalItem.isSetMaxAge()) {
               struct.put(READING_INTEREST_AGE_MAX, ION_SYSTEM.newInt(digitalItem.getMaxAge()));
            } else {
               struct.put(READING_INTEREST_AGE_MAX, ION_SYSTEM.newNullInt());
            }
            scope.put(READING_INTEREST_AGE, struct);
            ProfilerScope.addCounter("DBC.correctedReadingInterest", 0, "");
         }
      } else {
         scope.put(READING_INTEREST_AGE, ION_SYSTEM.newNullStruct());
      }
   }

   private static void addGradeLevelIon(final IonStruct scope, final DigitalItem digitalItem, final boolean isHeal) {
      if ((digitalItem.isSetMinGradeLevel() || digitalItem.isSetMaxGradeLevel())) {
         if (isHeal && (digitalItem.getAdultContentType() == AdultContentType.APPLICABLE || hasEroticaBISAC(digitalItem))) {
            scope.put(GRADE_LEVEL, ION_SYSTEM.newNullStruct());
            DOPS_DATA_CORRECTED.set(true);
            ProfilerScope.addCounter("DBC.correctedGradeLevel", 1, "");
         } else {
            IonStruct gradeLevel = ION_SYSTEM.newEmptyStruct();
            addString(gradeLevel, GRADE_LEVEL_MIN, digitalItem.getMinGradeLevel());
            addString(gradeLevel, GRADE_LEVEL_MAX, digitalItem.getMaxGradeLevel());
            scope.put(GRADE_LEVEL, gradeLevel);
            ProfilerScope.addCounter("DBC.correctedGradeLevel", 0, "");
         }
      } else {
         scope.put(GRADE_LEVEL, ION_SYSTEM.newNullStruct());
      }
   }
   
   public static boolean hasEroticaBISAC(DigitalItem digitalItem) {
      if (CollectionUtils.isEmpty(digitalItem.getCategories())) {
         return false;
      }
      for (BisacCategory category : (List<BisacCategory>)digitalItem.getCategories()) {
         if (EROTICA_BISAC_CODES.contains(category.getBisacCode())) {
            return true;
         }
      }
      return false;
   }

   private static void addCategoriesIon(final IonStruct scope, final List<Category> categories, final boolean isHeal) {
     IonList categoryList = ION_SYSTEM.newEmptyList();
     IonList tskList = ION_SYSTEM.newEmptyList();
     boolean correctedJuvenileCategories = false;
     for (Category category : categories) {
        BisacCategory bisacCategory = (BisacCategory) category;
        if (!StringUtils.isEmpty(bisacCategory.getBisacCode())) {
           if (!(isHeal && juvenileAdultContentCheck(scope, bisacCategory.getBisacCode()))) {
              if (CORRECTED_BISAC_CODES.containsKey(bisacCategory.getBisacCode())) {
                 categoryList.add(ION_SYSTEM.newString(CORRECTED_BISAC_CODES.get(bisacCategory.getBisacCode())));
              } else {
                 categoryList.add(ION_SYSTEM.newString(bisacCategory.getBisacCode()));
              }
              if (category instanceof BisacSubCategory) {
                 tskList.add(ION_SYSTEM.newString(
                  ((BisacSubCategory) bisacCategory).getSubCategory()));
              }
              else {
                 tskList.add(ION_SYSTEM.newNullString());
              }
              ProfilerScope.addCounter("DBC.correctedJUVCategories", 0, "");
           }
           else if (!correctedJuvenileCategories) {
              //corrected category bisac code + tsk
              categoryList.add(ION_SYSTEM.newString(FICTION_EROTICA_BISAC_CODE));
              tskList.add(ION_SYSTEM.newNullString());
              correctedJuvenileCategories = true;
              DOPS_DATA_CORRECTED.set(true);
              ProfilerScope.addCounter("DBC.correctedJUVCategories", 1, "");
           }
        }
     }
     scope.put(CATEGORIES, categoryList.isEmpty() ? ION_SYSTEM.newNullList() : categoryList);
     scope.put(THESAURUS_SUBJECT_KEYWORDS, tskList.isEmpty() ? ION_SYSTEM.newNullList() : tskList);
   }

   private static boolean juvenileAdultContentCheck(IonStruct scope, String bisacCode) {
      return !anyNull(scope.get(IS_ADULT_CONTENT)) && ((IonBool)scope.get(IS_ADULT_CONTENT)).booleanValue() && JUVENILE_BISAC_CODES.contains(bisacCode);
   }

   @VisibleForTesting
   protected static void addFeaturesIon(final IonStruct baseDigitalBookIon, final DigitalItem digitalItem) {
      Book book = InterfaceTypeUtil.getKDPBook(digitalItem);
      if (!digitalItem.isSetFeatures() && (book == null || book.getFanFictionBook() == null)) {
         return;
      }

      //features do not exist on model objects in DOPS and (unlike features like Manga) need to be created from other DOPI attributes
      IonList features = ION_SYSTEM.newEmptyList();
      if (digitalItem.isSetFeatures()) {
         for (Feature feature : (List<Feature>) digitalItem.getFeatures()) {
            if ("KW".equals(feature.getName())) {
               continue;
            }
            IonStruct featureIon = getFeatureIon(feature.getName(), feature.getMajorVersion(), feature.getMinorVersion(), feature.getSdl());
            if (feature.hasConfiguration()) {
               featureIon.add(FEATURE_CONFIGURATION, ION_SYSTEM.singleValue(feature.getConfiguration()));
            }
            features.add(featureIon);
         }
      }
      
      if (book != null && book.getFanFictionBook() != null) {
         features.add(getKWFeatureIon());
      }
      
      if (!features.isEmpty()) {
         baseDigitalBookIon.put(FEATURES, features);
      }
   }

   public static IonStruct getKWFeatureIon() {
      return getFeatureIon("KW", 0, 1, KW_DATA_SDL);
   }
   
   private static IonStruct getFeatureIon(String name, int major_version, int minor_version,
         String dataSdl) {
      IonStruct feature = ION_SYSTEM.newEmptyStruct();
      addString(feature, FEATURE_NAME, name);
      addInt(feature, FEATURE_MAJOR_VERSION, major_version);
      addInt(feature, FEATURE_MINOR_VERSION, minor_version);
      addStringIfNotNull(feature, FEATURE_DATA_SDL, dataSdl);
      return feature;
   }

   @VisibleForTesting
   protected static void addFeatureList(final TypeSafeIonStruct digitalBook, final DigitalItem item) {
      if (anyNull(digitalBook.get(FEATURES))) {
         return;
      }

      List<Feature> features = new ArrayList<>();
      IonList featuresIon = digitalBook.getList(FEATURES);
      for (IonValue fIon: featuresIon) {
         TypeSafeIonStruct fStruct = IonStructs.newTypeSafeStruct(fIon);
         Feature feature = new Feature();
         if ("KW".equals(getStringValue(fStruct, FEATURE_NAME))) {
            continue;
         }
         feature.setName(getStringValue(fStruct, FEATURE_NAME));
         feature.setSdl(getStringValue(fStruct, FEATURE_DATA_SDL));

         IonInt majorVersion = fStruct.getInt(FEATURE_MAJOR_VERSION);
         if (!anyNull(majorVersion)) {
            feature.setMajorVersion(majorVersion.intValue());
         }

         IonInt minorVersion = fStruct.getInt(FEATURE_MINOR_VERSION);
         if (!anyNull(minorVersion)) {
            feature.setMinorVersion(minorVersion.intValue());
         }

         TypeSafeIonStruct featureConfiguration = fStruct.getStruct(FEATURE_CONFIGURATION);
         if (!anyNull(featureConfiguration)) {
            StringBuilder configurationStringBuilder = new StringBuilder();
            try (IonWriter writer = ION_SYSTEM.newTextWriter(configurationStringBuilder)) {
               featureConfiguration.writeTo(writer);
            } catch (IOException e) {
               // Probably won't happen since we are writing to a StringBuilder
               throw new IonValueCastException(featureConfiguration, String.class);
            }
            feature.setConfiguration(configurationStringBuilder.toString());
         }

         features.add(feature);
      }

      if (!features.isEmpty()) {
         item.setFeatures(features);
      }
   }

   @VisibleForTesting
   protected static void addInternalPagesIon(final IonStruct scope, final List<InternalPage> internalPages) {
      if (internalPages == null || internalPages.isEmpty()) {
         return;
      }

      IonList internalPageListIon = ION_SYSTEM.newEmptyList();
      for (InternalPage internalPage : internalPages) {
         IonStruct internalPageIon = ION_SYSTEM.newEmptyStruct();
         addString(internalPageIon, INTERNAL_PAGE_ID, internalPage.getId());
         addInt(internalPageIon, INTERNAL_PAGE_VERSION, internalPage.getVersion());
         addString(internalPageIon, INTERNAL_PAGE_ASSET_TYPE, internalPage.getAssetType());
         internalPageListIon.add(internalPageIon);
      }
      scope.put(INTERNAL_PAGES, internalPageListIon);
   }

   // Populates contributors, primary_author fields in DBC. 
   // First author in contributor list is always primary_author
   private static void addContributorsIon(final IonStruct struct, List<Contributor> contributors) {
      IonList contributorsIon = ION_SYSTEM.newEmptyList();
      boolean primaryAuthorAdded = false;
      for (Contributor contributor : contributors) {
         if (ContributorType.AUTHOR == contributor.getType() && !primaryAuthorAdded) {
            primaryAuthorAdded = true;
            struct.put(PRIMARY_AUTHOR, getContributorIon(contributor));
         } else {
            contributorsIon.add(getContributorIon(contributor));
         }
      }
      if (!contributorsIon.isEmpty()) {
         struct.put(CONTRIBUTORS, contributorsIon);
      } else {
         struct.put(CONTRIBUTORS, ION_SYSTEM.newNullList());
      }
   }
   
   // List updates to DigitalBook 1.0 are overwrites, not merges, so we do not need to set
   // null values (otherwise set to signal a remove attribute on update/merge)
   private static IonValue getContributorIon(final Contributor contributor) {
      IonStruct contributorIon = ION_SYSTEM.newEmptyStruct();
      addString(contributorIon, CONTRIBUTOR_ROLE_CODE,
         KDP_CONTRIBUTOR_ROLE_CODES.get(contributor.getType().value));
      if (StringUtils.isNotBlank(contributor.getName()) && StringUtils.isNotBlank(contributor.getLastName())) {
         contributorIon.put(CONTRIBUTOR_FIRST_NAME, ION_SYSTEM.newString(contributor.getName()));
         contributorIon.put(CONTRIBUTOR_LAST_NAME, ION_SYSTEM.newString(contributor.getLastName()));
      } else if (StringUtils.isBlank(contributor.getLastName()) && StringUtils.isNotBlank(contributor.getName())) {
         contributorIon.put(CONTRIBUTOR_LAST_NAME, ION_SYSTEM.newString(contributor.getName()));
      } else if (StringUtils.isNotBlank(contributor.getLastName())) {
         contributorIon.put(CONTRIBUTOR_LAST_NAME, ION_SYSTEM.newString(contributor.getLastName()));
      }
      if (contributor.getContributorPronunciation() != null) {
         contributorIon.put(CONTRIBUTOR_PRONUNCIATION,
            ION_SYSTEM.newString(contributor.getContributorPronunciation()));
      }
      if (contributor.getContributorRomanized() != null) {
         contributorIon.put(CONTRIBUTOR_ROMANIZED,
            ION_SYSTEM.newString(contributor.getContributorRomanized()));
      }
      return contributorIon;
   }

   public static void addMetricsIon(final IonStruct val, final DigitalItem digitalItem) {
      IonStruct metrics = ION_SYSTEM.newEmptyStruct();
      ProgramDetail programDetail = digitalItem.getProgramDetail();
      if (programDetail != null && programDetail.hasAllExclusivePrograms() &&
            !programDetail.getAllExclusivePrograms().isEmpty()) {
         List<ExclusiveProgram> allPrograms = (List<ExclusiveProgram>) programDetail.getAllExclusivePrograms();
         ExclusiveProgram oldestProgram = findOldestProgram(allPrograms);
         if (oldestProgram != null && oldestProgram.getStartDate() != null) {
            metrics.put(METRIC_FIRST_TIME_SELECT_ACTIVE_DATE,
                  ION_SYSTEM.newTimestamp(Timestamp.forDateZ(oldestProgram.getStartDate())));
         }
      }
      if(digitalItem.getLiveDigitalItem() != null && digitalItem.getLiveDigitalItem().getFirstPublishedDate() != null) {
         metrics.put(METRIC_FIRST_PUBLISHED_DATE,
               ION_SYSTEM.newTimestamp(Timestamp.forDateZ(digitalItem.getLiveDigitalItem().getFirstPublishedDate())));
      }

      if (!metrics.isEmpty()) {
         val.add(METRICS, metrics);
      }
   }

   public static DigitalItem ionToDigitalItem(final IonValue value) {
      //Not used:
      //item.setAccountID();
      //item.setIsAdultContent();
      //item.setAdultContentTypeCorrection();
      //item.setComments();
      //item.setDigitalItemID();
      //item.setLastUpdatedDate();
      //item.setLiveVersion();
      //item.setMetadataVersion();
      //item.setPreorder();
      //item.setPublishingStatus();
      //item.setReferralClaimDate();
      //item.setReferralCode();
      //item.setRoyaltyUpdationInfo();
      //item.setStages();
      //item.setVersion();
      //item.setWordCount();
      
      //book.setDigitalItemTypeID();
      //book.setEditionRomanized();
      //book.setFanFictionBook();
      //book.setIsCoverImageIngested();
      //book.setPdDifferentiations();
      //book.setProductSiteLaunchDate();

      TypeSafeIonStruct digitalBook = IonStructs.newTypeSafeStruct(value);
      DigitalItem item = new DigitalItem();
      
      
      Book book = new Book();
      book.setType("Book");
      item.setDigitalItemType(book);
      final String itemSetId = digitalBook.getString(ITEM_SET_ID).stringValue();
      item.setItemSetID(itemSetId);
      final long digitalItemId = KDP_ITEM_SET_RESOURCE.getDigitalItemId(itemSetId);
      item.setDigitalItemID(digitalItemId);

      // Not used for KDP, but mimicking hibernate's defaulting
      List<BlockedReason> blockedReasons = Optional.ofNullable(digitalBook.getStruct(STATE))
            .map(state -> state.getList(STATE_BLOCKED_REASONS))
            .map(reasons -> reasons.stream().map(reason -> new BlockedReason()).collect(Collectors.toCollection(ArrayList::new)))
            .orElseGet(ArrayList::new);
      item.setBlockedReasons(blockedReasons);
      // Not used for KDP, but mimicking hibernate's defaulting
      item.setListPrices(new ArrayList<>());
      item.setProgramDetail(addSelectStatus(digitalBook, digitalItemId));

      // Cyborg has no non-test titles, always default to S3
      item.setStorageType(StorageType.S3);
      
      addDigitalOffer(digitalBook, item);
      book.setIsbn(getStringValue(digitalBook, ISBN));
      TypeSafeIonStruct matchbook = digitalBook.getStruct(MATCHBOOK);
      if (matchbook != null) {
         addAutoLitListing(item, matchbook);
      }
      else {
         item.setAutoLitListings(new ArrayList());
      }
      
      TypeSafeIonStruct draft = digitalBook.getStruct(DRAFT_SCOPE);
      if (!anyNull(draft)) {
         addDraftScopeAttributes(item, book, draft);
      } else {
         item.setSalesTerritories(new HashSet<>());
         item.setKeywords(new ArrayList<>());
         item.setContributors(new ArrayList<>());
         item.setThesaurusSubjectKeywords(new ArrayList<>());
         item.setRoyaltyPlanId("");
      }
      addLiveDigitalItem(digitalBook, item);
      
      addPreorder(digitalBook, item, book);
      
      addPublishingStatusAttribute(item, digitalBook);

      addFeatureList(digitalBook, item);
      
      IonTimestamp lastUpdatedDate = digitalBook.getTimestamp(LAST_CHANGED_DATE);
      if(!anyNull(lastUpdatedDate)) {
         item.setLastUpdatedDate(lastUpdatedDate.dateValue());
      }
      
      if (digitalBook.containsKey(FEATURES)) {
         IonList featuresIon = digitalBook.getList(FEATURES);
         for (IonValue feature : featuresIon) {
            IonStruct featureIon = (IonStruct) feature;
            IonString name = (IonString)featureIon.get(FEATURE_NAME);
            if (!anyNull(name) && "KW".equals(name.stringValue())) {
               addKWAttributes(digitalBook, item, book);
            }
         }
      }
      
      return item;
   }
   
   private static Map<String, String> BISAC_CODE_TO_LITERAL = new ConcurrentHashMap<String, String>();
   
   private static Function<String, String> bisacCodeLookupFunction = new Function<String,String>() {
      
      @Override
      public String apply(String t) {
         try {
            IonStruct node = (IonStruct)Singletons.getCardinalClient().initiate(getBisacRequest(t)).getEntity();
            IonList parts = (IonList)node.get("literal_parts");
            if(!anyNull(parts)) {
               return StringUtils.join(parts.stream().map(part -> ((IonString)part).stringValue()).collect(Collectors.toList()), " / ");
            }
         }
         catch(NotFoundException e) {
         }
         catch (IOException e) {
            throw new RuntimeException(e);
         }
         return null;
      }
      
      private CardinalRequest getBisacRequest(String bisacCode) {
         return new CardinalRequest(RequestType.POST, "DigitalBook", "2.0", "CategoryNode").withEntity(
               Singletons.getIonSystem().singleValue("{ language:\"en_US\", code:\"" + bisacCode +"\", }"));
      }
   };
   
   //TODO: Fetch category names from KDPPrintBook/1.0/CategoryNode if we need to render the full name in CAPA...also for the 'draft' DigitalItem
   private static void addLiveDigitalItem(TypeSafeIonStruct digitalBook, DigitalItem item) {
      TypeSafeIonStruct live = digitalBook.getStruct(LIVE_SCOPE);
      if (!anyNull(live)) {
         LiveDigitalItem liveItem = new LiveDigitalItem();
         
         liveItem.setTitle(!anyNull(live.getString(TITLE)) ? live.getString(TITLE).stringValue() : null);
         liveItem.setTitlePronunciation(!anyNull(live.getString(TITLE_PRONUNCIATION)) ? live.getString(TITLE_PRONUNCIATION).stringValue() : null);
         liveItem.setTitleRomanized(!anyNull(live.getString(TITLE_ROMANIZED)) ? live.getString(TITLE_ROMANIZED).stringValue() : null);
         liveItem.setSubtitle(!anyNull(live.getString(SUBTITLE)) ? live.getString(SUBTITLE).stringValue() : null);
         liveItem.setSubtitlePronunciation(!anyNull(live.getString(SUBTITLE_PRONUNCIATION)) ? live.getString(SUBTITLE_PRONUNCIATION).stringValue() : null);
         liveItem.setSubtitleRomanized(!anyNull(live.getString(SUBTITLE_ROMANIZED)) ? live.getString(SUBTITLE_ROMANIZED).stringValue() : null);
         liveItem.setPublisher(!anyNull(live.getString(PUBLISHER)) ? live.getString(PUBLISHER).stringValue() : null);
         liveItem.setPublisherLabel(!anyNull(live.getString(PUBLISHER_LABEL)) ? live.getString(PUBLISHER_LABEL).stringValue() : null);
         liveItem.setPublisherLabelPronunciation(!anyNull(live.getString(PUBLISHER_LABEL_PRONUNCIATION)) ? live.getString(PUBLISHER_LABEL_PRONUNCIATION).stringValue() : null);
         liveItem.setSeriesTitle(!anyNull(live.getString(SERIES_TITLE)) ? live.getString(SERIES_TITLE).stringValue() : null);
         liveItem.setSeriesTitlePronunciation(!anyNull(live.getString(SERIES_TITLE_PRONUNCIATION)) ? live.getString(SERIES_TITLE_PRONUNCIATION).stringValue() : null);
         liveItem.setSeriesTitleRomanized(!anyNull(live.getString(SERIES_TITLE_ROMANIZED)) ? live.getString(SERIES_TITLE_ROMANIZED).stringValue() : null);
         liveItem.setSeriesVolume(
               !anyNull(live.getInt(SERIES_NUMBER)) ? Integer.toString(live.getInt(SERIES_NUMBER).intValue()) :
                  !anyNull(live.getString(SERIES_VOLUME)) ? live.getString(SERIES_VOLUME).stringValue() : null);
         liveItem.setEdition(!anyNull(live.getString(EDITION)) ? live.getString(EDITION).stringValue() : null);
         liveItem.setProductDescription(!anyNull(live.getString(DESCRIPTION)) ? live.getString(DESCRIPTION).stringValue() : null);
         liveItem.setLanguage(!anyNull(live.getString(LANGUAGE)) ? lookupLanguage(live.getString(LANGUAGE).stringValue()) : null);
         liveItem.setPageTurnDirection(!anyNull(live.getString(PAGE_TURN_DIRECTION)) ? live.getString(PAGE_TURN_DIRECTION).stringValue() : null);
         liveItem.setIsbn(!anyNull(live.getString(ISBN)) ? live.getString(ISBN).stringValue() : null);
         liveItem.setPublishingRights(
               !anyNull(live.getBool(IS_PUBLIC_DOMAIN)) ? live.getBool(IS_PUBLIC_DOMAIN).booleanValue() ? PublishingRightsStatus.PUBLIC_DOMAIN : PublishingRightsStatus.NOT_APPLICABLE : null);
         liveItem.setKeywords(!anyNull(live.getList(KEYWORDS)) ? live.getList(KEYWORDS).stream().map(keywordIon -> ((IonString)keywordIon).stringValue()).collect(Collectors.toList()) : new ArrayList<>());
         liveItem.setAdultContentType(
               !anyNull(live.getBool(IS_ADULT_CONTENT)) ? live.getBool(IS_ADULT_CONTENT).booleanValue() ? AdultContentType.APPLICABLE : AdultContentType.NOT_APPLICABLE : null);
         liveItem.setLendingPlan(
               !anyNull(live.getBool(IS_BOOK_LENDING)) ? live.getBool(IS_BOOK_LENDING).booleanValue() ? LendingPlan.BASIC : LendingPlan.NONE : null);
         
         
         IonList categories = live.getList(CATEGORIES);
         if(!anyNull(categories)) {
            List<LiveCategory> liveCategories = new ArrayList<>();
            for(IonValue category : categories) {
               String bisacCode = ((IonString)category).stringValue();
               LiveCategory liveCategory = new LiveCategory();
               liveCategory.setBisacCode(bisacCode);
               liveCategory.setCategoryName(BISAC_CODE_TO_LITERAL.computeIfAbsent(bisacCode, bisacCodeLookupFunction));
               liveCategories.add(liveCategory);
            }
            liveItem.setLiveCategories(liveCategories);
         }

         IonTimestamp publicationDate = live.getTimestamp(PUBLICATION_DATE);
         if (!anyNull(publicationDate)) {
            liveItem.setPublicationDate(publicationDate.dateValue());
            item.setPublicationDate(publicationDate.dateValue());
            InterfaceTypeUtil.getKDPBook(item).setPublicationDate(publicationDate.dateValue());
         }
         
         TypeSafeIonStruct liveS3PublisherCover = live.getStruct(S3_PUBLISHER_COVER);
         if (!anyNull(liveS3PublisherCover) && !anyNull(liveS3PublisherCover.getInt("version"))) {
            liveItem.setProductImageVersion(liveS3PublisherCover.getInt("version").intValue());
         }
         
         addLiveChargeMethods(live, liveItem);
         
         addLiveContributorList(live, liveItem);
         
         IonDecimal royaltyRate = live.getDecimal(ROYALTY_RATE);
         if (!anyNull(royaltyRate)) {
            liveItem.setRoyaltyPlanId(ROYALTY_PLAN_TO_RATE_MAPPING.inverse()
               .get(royaltyRate.doubleValue()));
         }
         
         IonBool isDRM = live.getBool(IS_DRM);
         if (!anyNull(isDRM)) {
            liveItem.setIsDRM(isDRM.booleanValue());
         }
         
         IonString digitalBookStatus = digitalBook.getString("status");
         if (!anyNull(digitalBookStatus) && "live_unpublished".equals(digitalBookStatus.stringValue())) {
            liveItem.setLiveItemStatus(LiveItemStatus.UNPUBLISHED);
         }
         
         TypeSafeIonStruct metricsStruct = digitalBook.getStruct(METRICS);
         if (!anyNull(metricsStruct)) {
            IonTimestamp firstPublishedDate = metricsStruct.getTimestamp(METRIC_FIRST_PUBLISHED_DATE);
            if (!anyNull(firstPublishedDate)) {
               liveItem.setFirstPublishedDate(firstPublishedDate.dateValue());
            }
         }
         
         TypeSafeIonStruct stateStruct = digitalBook.getStruct(STATE);
         if(!anyNull(stateStruct)) {
            IonTimestamp lastPublishedDate = stateStruct.getTimestamp(STATE_PUBLISHING_START_DATE);
            if (!anyNull(lastPublishedDate)) {
               liveItem.setLastPublishedDate(lastPublishedDate.dateValue());
            }
         }
         
         item.setLiveDigitalItem(liveItem);
      }
   }

   private static void addLiveContributorList(final TypeSafeIonStruct live, final LiveDigitalItem liveItem) {
      List<LiveContributor> contributors = new ArrayList<>();
      TypeSafeIonStruct primaryAuthor = live.getStruct(PRIMARY_AUTHOR);
      if (!anyNull(primaryAuthor)) {
         addLiveContributor(contributors, primaryAuthor);
      }
      IonList contributorsIon = live.getList(CONTRIBUTORS);
      if (!anyNull(contributorsIon)) {
         for (IonValue contributorIon : contributorsIon) {
            addLiveContributor(contributors, IonStructs.newTypeSafeStruct(contributorIon));
         }
      }
      liveItem.setLiveContributors(contributors.isEmpty() ? null : contributors);
   }

   private static void addLiveContributor(List<LiveContributor> contributors,
         TypeSafeIonStruct struct) {
      //Not used:
      //contributor.setContributorID();
      
      LiveContributor contributor = new LiveContributor();
      IonString roleCode = struct.getString(CONTRIBUTOR_ROLE_CODE);
      if (!anyNull(roleCode)) {
         contributor.setType(ContributorType.fromValue(
            KDP_CONTRIBUTOR_ROLE_CODES.inverse().get(roleCode.stringValue())));
      }
      contributor.setName(getStringValue(struct, CONTRIBUTOR_FIRST_NAME));
      contributor.setLastName(getStringValue(struct, CONTRIBUTOR_LAST_NAME));
      contributor.setContributorPronunciation(getStringValue(struct, CONTRIBUTOR_PRONUNCIATION));
      contributor.setContributorRomanized(getStringValue(struct, CONTRIBUTOR_ROMANIZED));
      contributors.add(contributor);
   }

   private static void addLiveChargeMethods(TypeSafeIonStruct live,
         LiveDigitalItem liveItem) {
      List<LiveChargeMethod> chargeMethods = new ArrayList<>();
      TypeSafeIonStruct amazonChannelIon = 
         live.getStruct(AMAZON_CHANNEL);
      if (anyNull(amazonChannelIon)) {
         return;
      }
      for (String marketplace : MARKETPLACE_CURRENCY_CODES.keySet()) {
         TypeSafeIonStruct marketplaceIon = amazonChannelIon.getStruct(marketplace);
         if (!anyNull(marketplaceIon)) {
            LiveChargeMethod chargeMethod = new LiveChargeMethod();
            //Not used:
            //chargeMethod.setChargeMethodID();
            //chargeMethod.setKcdPriceChangedDate
            IonTimestamp updateDate = marketplaceIon.getTimestamp(LAST_CHANGED_DATE);
            IonTimestamp priceChangeDate = marketplaceIon.getTimestamp(PRICE_CHANGE_DATE);
            if (!anyNull(priceChangeDate)) {
               chargeMethod.setKcdPriceChangedDate(new Date(priceChangeDate.getMillis()));
            }
            else {
               chargeMethod.setKcdPriceChangedDate(new Date(updateDate.getMillis()));
            }
            IonDecimal price = marketplaceIon.getDecimal(PRICE_VAT_EXCLUSIVE);
            if (price != null) {
               switch (marketplace) {
               //TODO verify we do not need to support CN
                  case "US":
                     liveItem.setPrice(price.doubleValue());
                     break;
                  case "UK":
                     liveItem.setPriceUK(price.doubleValue());
                     break;
                  case "DE":
                     liveItem.setPriceDE(price.doubleValue());
                     break;
                  case "FR":
                     liveItem.setPriceFR(price.doubleValue());
                     break;
                  case "ES":
                     liveItem.setPriceES(price.doubleValue());
                     break;
                  case "IN":
                     liveItem.setPriceIN(price.doubleValue());
                     break;
                  case "IT":
                     liveItem.setPriceIT(price.doubleValue());
                     break;
                  case "JP":
                     liveItem.setPriceJP(price.doubleValue());
                     break;
                  case "CA":
                     liveItem.setPriceCA(price.doubleValue());
                     break;
                  case "BR":
                     liveItem.setPriceBR(price.doubleValue());
                     break;
                  case "MX":
                     liveItem.setPriceMX(price.doubleValue());
                     break;
                  case "AU":
                     liveItem.setPriceAU(price.doubleValue());
                     break;
                  case "NL":
                     liveItem.setPriceNL(price.doubleValue());
                     break;
                  default:
                     break;
               }
            }
            IonDecimal priceWithTax = marketplaceIon.getDecimal(PRICE_VAT_INCLUSIVE);
            // Marketplaces with no VAT do not have priceWithTax set by DOPS on returned value
            if (!anyNull(priceWithTax) && VAT_MARKETPLACES.contains(marketplace)) {
               chargeMethod.setPriceWithTax(priceWithTax.doubleValue());
            }
            chargeMethod.setType(CHARGE_METHOD_MARKETPLACES.inverse().get(marketplace));
            chargeMethods.add(chargeMethod);
         }
      }
      liveItem.setLiveChargeMethods(chargeMethods);
   }

   private static void addKWAttributes(TypeSafeIonStruct digitalBook,
         DigitalItem item, Book book) {
      //Not used: LiveFanFictionBook (not referenced outside of DOPS, not in DOPI)
      FanFictionBook fanFictionBook = new FanFictionBook();
      
      TypeSafeIonStruct state = digitalBook.getStruct(STATE);
      if (!anyNull(state)) {
         IonTimestamp reviewStartDate = state.getTimestamp(STATE_REVIEW_START_DATE);
         if (!anyNull(reviewStartDate)) {
            // lastSubmittedDate is set on the book in a save call prior to the DOPS publish call in Potter.
            // This attribute has not been added in DBC in favor of the review_start_date set on submit for review
            fanFictionBook.setLastSubmittedDate(reviewStartDate.dateValue());
         }
      }
      TypeSafeIonStruct draft = digitalBook.getStruct(DRAFT_SCOPE);
      if (!anyNull(draft)) {
         if (!anyNull(draft.get(KW_UNIVERSE_ID))) {
            fanFictionBook.setUniverseId(draft.getString(KW_UNIVERSE_ID).stringValue());
         }
         if (!anyNull(draft.get(KW_UNIVERSE_NAME))) {
            fanFictionBook.setUniverseName(draft.getString(KW_UNIVERSE_NAME).stringValue());
         }
         if (!anyNull(draft.get(KW_CONTENT_LENGTH))) {
            fanFictionBook.setContentLength(draft.getString(KW_CONTENT_LENGTH).stringValue());
         }
         if (!anyNull(draft.get(KW_WORD_COUNT))) {
            item.setWordCount(draft.getInt(KW_WORD_COUNT).intValue());
         }
      }
      book.setFanFictionBook(fanFictionBook);
   }
   
   protected static ProgramDetail addSelectStatus(final TypeSafeIonStruct digitalBook,
                                                final Long digitalItemId) {
      TypeSafeIonStruct selectStatus = digitalBook.getStruct(SELECT_STATUS);
      if (selectStatus == null) {
         String publishingStatus = digitalBook.containsKey(ITEM_STATUS) ?
               digitalBook.getString(ITEM_STATUS).stringValue() : null;
         boolean isTopaz = false;
         TypeSafeIonStruct draft = digitalBook.getStruct(DRAFT_SCOPE);
         if (draft != null) {
            TypeSafeIonStruct fileProcessing = draft.getStruct(FILE_PROCESSING);
            if (fileProcessing != null) {
               TypeSafeIonStruct s3Asset = fileProcessing.getStruct("s3_converted_asset");
               if (s3Asset != null) {
                  IonString mediaType = s3Asset.getString("media_type");
                  if (mediaType != null && "tpz".equals(mediaType.stringValue())) {
                     isTopaz = true;
                  }
               }
               TypeSafeIonStruct asset = fileProcessing.getStruct("converted_asset");
               if (asset != null) {
                  IonString mediaType = asset.getString("media_type");
                  if (mediaType != null && "tpz".equals(mediaType.stringValue())) {
                     isTopaz = true;
                  }
               }
            }
         }
         boolean eligible = !isTopaz &&
               (publishingStatus == null ||
                (!publishingStatus.equals("deleted") &&
                 !publishingStatus.equals("live_blocked") &&
                 !publishingStatus.equals("draft_blocked")));
         ProgramDetail programDetail = new ProgramDetail();
         programDetail.setDigitalItemId(digitalItemId);
         programDetail.setIsItemEligible(eligible);
         programDetail.setIsItemExclusive(false);
         return programDetail;
      }

      if (selectStatus.containsKey(SELECT_STATUS_ELIGIBILITY_OVERRIDE_DATE) ||
          selectStatus.containsKey(SELECT_STATUS_ELIGIBILITY_OVERRIDE_REASON)) {
         TypeSafeIonStruct tmp = IonStructs.newTypeSafeStruct(selectStatus.clone());
         tmp.removeAll(SELECT_STATUS_STATUS,
               SELECT_STATUS_ELIGIBLE,
               SELECT_STATUS_ELIGIBILITY_OVERRIDE_DATE,
               SELECT_STATUS_ELIGIBILITY_OVERRIDE_REASON,
               SELECT_STATUS_WINDOW_ID,
               LAST_CHANGED_DATE
          );
         if (tmp.isEmpty()) {
            ProgramDetail programDetail = new ProgramDetail();
            programDetail.setDigitalItemId(digitalItemId);
            programDetail.setIsItemEligible(selectStatus.getBool(SELECT_STATUS_ELIGIBLE).booleanValue());
            programDetail.setIsItemExclusive(false);
            return programDetail;
         }
      }

      Date now = new Date();
      ExclusiveProgram exclusiveProgram = new ExclusiveProgram();

      if (selectStatus.containsKey(SELECT_STATUS_AUTO_RENEW)) {
         exclusiveProgram.setIsRenewable(
            selectStatus.getBool(SELECT_STATUS_AUTO_RENEW).booleanValue());
      }

      final ExclusiveProgramStatus exclusiveProgramStatus =
              ExclusiveProgramStatus.fromValue(
                      selectStatus.getString(SELECT_STATUS_STATUS).stringValue().toUpperCase());
      exclusiveProgram.setProgramStatus(exclusiveProgramStatus);

      if (selectStatus.containsKey(SELECT_STATUS_ENROLLED_DATE)) {
         Date enrollmentDate = selectStatus.getTimestamp(SELECT_STATUS_ENROLLED_DATE).dateValue();
         if (ExclusiveProgramStatus.ACTIVE == exclusiveProgramStatus ||
             ExclusiveProgramStatus.UNPUBLISHED == exclusiveProgramStatus) {
            Date startDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
            Date startDateForCurrentPeriod = advanceStartDateToPeriod(startDate, now);
            if (startDate != startDateForCurrentPeriod) {
               enrollmentDate = startDateForCurrentPeriod;
            }
         }
         else if (ExclusiveProgramStatus.EXPIRED == exclusiveProgramStatus) {
            Date endDate = selectStatus.getTimestamp(SELECT_STATUS_CURRENT_END_DATE).dateValue();
            Date startDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
            Date startDateForCurrentPeriod = advanceStartDateToPeriod(startDate, endDate);
            if (startDate != startDateForCurrentPeriod) {
               enrollmentDate = startDateForCurrentPeriod;
            }
         }
         else if (ExclusiveProgramStatus.OPTED_OUT == exclusiveProgramStatus) {
            Date optedOutDate = selectStatus.getTimestamp(SELECT_STATUS_OPT_OUT_DATE).dateValue();
            Date startDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
            Date startDateForCurrentPeriod = advanceStartDateToPeriod(startDate, optedOutDate);
            if (startDate != startDateForCurrentPeriod) {
               enrollmentDate = startDateForCurrentPeriod;
            }
         }
         else if (ExclusiveProgramStatus.TERMINATED == exclusiveProgramStatus) {
            Date terminatedDate = selectStatus.getTimestamp(SELECT_STATUS_TERMINATED_DATE).dateValue();
            if (selectStatus.containsKey(SELECT_STATUS_FIRST_ACTIVE_DATE)) {
               Date startDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
               Date startDateForCurrentPeriod = advanceStartDateToPeriod(startDate, terminatedDate);
               if (startDate != startDateForCurrentPeriod) {
                  enrollmentDate = startDateForCurrentPeriod;
               }
            }
         }
         exclusiveProgram.setEnrollmentDate(enrollmentDate);
      }

      if (selectStatus.containsKey(SELECT_STATUS_FIRST_ACTIVE_DATE)) {
         Date activeDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
         if (ExclusiveProgramStatus.ACTIVE == exclusiveProgramStatus ||
             ExclusiveProgramStatus.UNPUBLISHED == exclusiveProgramStatus) {
            exclusiveProgram.setStartDate(advanceStartDateToPeriod(activeDate, now));
         }
         else if (ExclusiveProgramStatus.EXPIRED == exclusiveProgramStatus) {
            Date endDate = selectStatus.getTimestamp(SELECT_STATUS_CURRENT_END_DATE).dateValue();
            exclusiveProgram.setStartDate(advanceStartDateToPeriod(activeDate, endDate));
         }
         else if (ExclusiveProgramStatus.OPTED_OUT == exclusiveProgramStatus) {
            Date optedOutDate = selectStatus.getTimestamp(SELECT_STATUS_OPT_OUT_DATE).dateValue();
            exclusiveProgram.setStartDate(advanceStartDateToPeriod(activeDate, optedOutDate));
         }
         else if (ExclusiveProgramStatus.TERMINATED == exclusiveProgramStatus) {
            Date terminatedDate = selectStatus.getTimestamp(SELECT_STATUS_TERMINATED_DATE).dateValue();
            exclusiveProgram.setStartDate(advanceStartDateToPeriod(activeDate, terminatedDate));
         }
      }

      if (ExclusiveProgramStatus.OPTED_OUT == exclusiveProgramStatus) {
         Date optedOutDate = selectStatus.getTimestamp(SELECT_STATUS_OPT_OUT_DATE).dateValue();
         exclusiveProgram.setEndDate(optedOutDate);
      }
      else if (ExclusiveProgramStatus.TERMINATED == exclusiveProgramStatus) {
         if (selectStatus.containsKey(SELECT_STATUS_FIRST_ACTIVE_DATE)) {
            Date terminatedDate = selectStatus.getTimestamp(SELECT_STATUS_TERMINATED_DATE).dateValue();
            exclusiveProgram.setEndDate(terminatedDate);
         }
      }
      else if (selectStatus.containsKey(SELECT_STATUS_CURRENT_END_DATE)) {
         exclusiveProgram.setEndDate(
            selectStatus.getTimestamp(SELECT_STATUS_CURRENT_END_DATE).dateValue());
      }
      else if (ExclusiveProgramStatus.ACTIVE == exclusiveProgramStatus ||
            ExclusiveProgramStatus.UNPUBLISHED == exclusiveProgramStatus) {
         Date startDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
         Date currentStartDate = advanceStartDateToPeriod(startDate, now);
         exclusiveProgram.setEndDate(getNextEndDate(currentStartDate));
      }

      ProgramDetail programDetail = new ProgramDetail();

      programDetail.setLatestExclusiveProgram(exclusiveProgram);

      List<ExclusiveProgram> contiguousPrograms = reconstructContiguousPrograms(selectStatus, exclusiveProgram);
      programDetail.setAllExclusivePrograms(contiguousPrograms);

      if (selectStatus.containsKey(SELECT_STATUS_ELIGIBLE)) {
         programDetail.setIsItemEligible(selectStatus.getBool(SELECT_STATUS_ELIGIBLE).booleanValue());
      }

      if (ExclusiveProgramStatus.ACTIVE == exclusiveProgramStatus ||
          ExclusiveProgramStatus.UNPUBLISHED == exclusiveProgramStatus) {
         Date startDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
         Date currentPeriodStartDate = advanceStartDateToPeriod(startDate, now);
         
         int daysUsed = 0;
         Timestamp start = Timestamp.forDateZ(currentPeriodStartDate);
         String key = String.format("%d-%02d-%02d", start.getYear(), start.getMonth(), start.getDay());
         IonValue freePromotions = digitalBook.get("free_promotions");
         if (!IonValueUtils.anyNull(freePromotions) && IonType.STRUCT.equals(freePromotions.getType())) {
            TypeSafeIonStruct period = IonStructs.newTypeSafeStruct(freePromotions).getStruct(key);
            if (!IonValueUtils.anyNull(period)) {
               TypeSafeIonStruct promotions = period.getStruct("promotions");
               if (!IonValueUtils.anyNull(promotions)) {
                  Iterator<IonValue> iter = promotions.iterator();
                  while (iter.hasNext()) {
                     IonStruct promo = (IonStruct) iter.next();
                     if (!IonValueUtils.anyNull(promo) &&
                         promo.containsKey("duration") &&
                         !IonValueUtils.anyNull(promo.get("duration")) &&
                         !IonValueUtils.anyNull(promo.get("start_date"))) {
                        Date periodStartDate = ((IonTimestamp) promo.get("start_date")).dateValue();
                        if (now.after(periodStartDate)) {
                           daysUsed += ((IonInt) promo.get("duration")).intValue();
                        }
                     }
                  }
               }
            }
         }
         if (now.after(adjustDate(currentPeriodStartDate, Calendar.DATE, 3)) || daysUsed > 0) {
            programDetail.setExclusivityGracePeriodStatus(ExclusivityGracePeriodStatus.OVER);
         }
         else if (!IonValueUtils.anyNull(selectStatus.getTimestamp(SELECT_STATUS_OPT_OUT_DATE)) &&
                  selectStatus.getTimestamp(SELECT_STATUS_OPT_OUT_DATE).dateValue()
                     .after(adjustDate (currentPeriodStartDate, Calendar.DATE, -90))) {
            programDetail.setExclusivityGracePeriodStatus(ExclusivityGracePeriodStatus.USED);
         }
         else {
            programDetail.setExclusivityGracePeriodStatus(ExclusivityGracePeriodStatus.IN_PROGRESS);
         }
      }
      else if (ExclusiveProgramStatus.OPTED_OUT == exclusiveProgramStatus) {
         programDetail.setExclusivityGracePeriodStatus(ExclusivityGracePeriodStatus.USED);
      }
      else {
         programDetail.setExclusivityGracePeriodStatus(ExclusivityGracePeriodStatus.NOT_APPLICABLE);
      }

      // see DigitalItem::isExclusive in DOPS
      programDetail.setIsItemExclusive(
              ExclusiveProgramStatus.DRAFT.equals(exclusiveProgramStatus)  ||
              ExclusiveProgramStatus.ACTIVE.equals(exclusiveProgramStatus) ||
              ExclusiveProgramStatus.UNPUBLISHED.equals(exclusiveProgramStatus));

      programDetail.setDigitalItemId(digitalItemId);

      return programDetail;
   }

   private static List<ExclusiveProgram> reconstructContiguousPrograms(TypeSafeIonStruct selectStatus, ExclusiveProgram recentProgram) {
      List<ExclusiveProgram> result = new ArrayList<>();

      if (recentProgram.getProgramStatus() == ExclusiveProgramStatus.ACTIVE || recentProgram.getProgramStatus() == ExclusiveProgramStatus.UNPUBLISHED) {
         if (selectStatus.containsKey(SELECT_STATUS_ENROLLED_DATE) && selectStatus.containsKey(SELECT_STATUS_FIRST_ACTIVE_DATE)) {
            Date enrollmentDate = selectStatus.getTimestamp(SELECT_STATUS_ENROLLED_DATE).dateValue();
            Date firstActiveDate = selectStatus.getTimestamp(SELECT_STATUS_FIRST_ACTIVE_DATE).dateValue();
            ExclusiveProgram currentProgram = new ExclusiveProgram();

            currentProgram.setEnrollmentDate(enrollmentDate);
            currentProgram.setStartDate(firstActiveDate);
            currentProgram.setEndDate(getNextEndDate(firstActiveDate));
            currentProgram.setProgramStatus(ExclusiveProgramStatus.EXPIRED);
            currentProgram.setIsRenewable(true);

            while (currentProgram.getEndDate().before(recentProgram.getStartDate())) {
               result.add(currentProgram);
               Date startDate = getNextStartDate(currentProgram.getStartDate());
               Date endDate = getNextEndDate(startDate);

               currentProgram = new ExclusiveProgram();
               currentProgram.setEnrollmentDate(startDate);
               currentProgram.setStartDate(startDate);
               currentProgram.setEndDate(endDate);
               currentProgram.setProgramStatus(ExclusiveProgramStatus.EXPIRED);
               currentProgram.setIsRenewable(true);
            }
         }
      }
      result.add(recentProgram);
      Collections.reverse(result);
      return result;
   }

   private static Date adjustDate(final Date date, final int unitType, final int units) {
      final Calendar c = Calendar.getInstance();
      c.setTime(date);
      c.add(unitType, units);
      return c.getTime();
   }

   public static Date advanceStartDateToPeriod(Date startDate, Date target) {
      Date currentStartDate = startDate;
      Date nextStartDate = null;
      do {
         nextStartDate = getNextStartDate(currentStartDate);
         if (nextStartDate.before(target)) {
            currentStartDate = nextStartDate;
         }
      } while (nextStartDate.before(target));
      
      return currentStartDate;
   }
   
   public static Date getNextStartDate(Date startDate) {
      final Calendar cal = Calendar.getInstance();
      cal.setTime(startDate);
      cal.add(Calendar.DATE, 90);
      
      DateTime endDate = new DateTime(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
      endDate = endDate.withDate(cal.get(Calendar.YEAR),
                                 cal.get(Calendar.MONTH) + 1,
                                 cal.get(Calendar.DATE));
      endDate = endDate.withTime(0, 0, 0, 0);
      return endDate.toDate();
   }

   public static Date getNextEndDate(Date startDate) {
      Date nextStartDate = getNextStartDate(startDate);
      final Calendar cal = Calendar.getInstance();
      cal.setTime(nextStartDate);
      cal.add(Calendar.SECOND, -1);
      return cal.getTime();
   }

   private static void addPublishingStatusAttribute(final DigitalItem item, final TypeSafeIonStruct digitalBook) {
      IonString ionStatus = digitalBook.getString("status");
      if(!IonValueUtils.anyNull(ionStatus)) {
         String status = ionStatus.stringValue();
         PublishingStatus publishingStatus = DIGITAL_ITEM_STATUS_MAP.get(status);
         
         TypeSafeIonStruct dbcState = digitalBook.getStruct("state");
         boolean wasUnpublished = !anyNull(dbcState) && !anyNull(dbcState.getTimestamp("unpublished_date"));
         
         if (item.getLiveDigitalItem() != null) {
            if(DIGITAL_ITEM_LIVE_STATUS_MAP.get(status) != null) {
               item.getLiveDigitalItem().setLiveItemStatus(DIGITAL_ITEM_LIVE_STATUS_MAP.get(status));
            }
            else if(wasUnpublished) {
               item.getLiveDigitalItem().setLiveItemStatus(LiveItemStatus.UNPUBLISHED);
            }
         }
         if(publishingStatus == PublishingStatus.DRAFT && isReady(item, digitalBook)) {
            publishingStatus = PublishingStatus.READY;
         }
         item.setPublishingStatus(publishingStatus);
      }
   }
   
   private static boolean isReady(DigitalItem item, TypeSafeIonStruct digitalBook) {
      //DigitalOpenPublishingService/src/amazon/dtp/dops/controllers/helpers/BookStateTransition.java > isProductDetailsAvailable()
      if (item.getContributors() == null || item.getContributors().isEmpty()) {
         return false;
      }
      if (item.getCategories() == null || item.getCategories().isEmpty()) {
         return false;
      }
      Book book = (Book) item.getDigitalItemType();
      if (!book.isSetIsDRM()) {
         return false;
      }
      if (book.getPublishingRights() == null) {
         return false;
      }
      if (item.getPreorder() != null && item.getPreorder().getReleaseDate() == null) {
         return false;
      }
      
      if (StringUtils.isBlank(item.getTitle()) || item.getTitle().matches("New Title \\d+")) {
         return false;
      }
      if (StringUtils.isBlank(item.getProductDescription())) {
         return false;
      }
      //DOPS checks description length in bytes <= 4000 which is handled by DBC's SDL
      //DOPS checks description doesn't contain invalid HTML which is handled by DBC's SDL
      if (StringUtils.isBlank(item.getLanguage())) {
         return false;
      }
      //DOPS checks language is in the DigitalItemLanguage enum set which is defined in DBC's SDL
      
      //DigitalOpenPublishingService/src/amazon/dtp/dops/controllers/helpers/BookStateTransition.java > isTerritoryRightsAvailable()
//      if (!item.getIsSalesRightsAccepted()) {
//         return false;
//      }
      if (item.getSalesTerritories() == null || item.getSalesTerritories().isEmpty()) {
         return false;
      }

      //DigitalOpenPublishingService/src/amazon/dtp/dops/controllers/helpers/BookStateTransition.java > isSystemContentAvailable()
      if (StorageType.CYBORG.equals(item.getStorageType())) {
         return false;
      }
      TypeSafeIonStruct draft = digitalBook.getStruct("draft");
      if (IonValueUtils.anyNull(draft)) {
         return false;
      }
      TypeSafeIonStruct fileProcessing = draft.getStruct("file_processing");
      //Assetless preorder support: Do not require file_processing struct for READY future preorder titles unless an interior file is provided
      boolean isFuturePreorder = item.getPreorder() != null && item.getPreorder().getReleaseDate() != null 
            && item.getPreorder().getReleaseDate().after(new Date()) && !item.getPreorder().isSetCancellationDate();
      if (!(isFuturePreorder && (IonValueUtils.anyNull(fileProcessing) || IonValueUtils.anyNull(fileProcessing.getStruct("s3_converted_asset"))))) {
         if (IonValueUtils.anyNull(fileProcessing)) {
            return false;
         }
         IonString ionFileProcessingStatus = fileProcessing.getString("status");
         if (IonValueUtils.anyNull(ionFileProcessingStatus)
               || !"COMPLETED".equals(ionFileProcessingStatus.stringValue())) {
            return false;
         }
         if (IonValueUtils.anyNull(fileProcessing.getInt("file_size_bytes"))) {
            return false;
         }
         if (IonValueUtils.anyNull(fileProcessing.getStruct("s3_converted_asset"))) {
            return false;
         }
      }
      
      //DigitalOpenPublishingService/src/amazon/dtp/dops/controllers/helpers/BookStateTransition.java > isProgramsDataAvailable()
      //DOPS verifies stage statuses are 'complete'.
      if(StringUtils.isBlank(item.getRoyaltyPlanId())) {
         return false;
      }
      if(item.getCurrentOffers() == null || item.getCurrentOffers().isEmpty()) {
         return false;
      }
      DigitalOffer offer = (DigitalOffer)item.getCurrentOffers().get(0);
      if(offer == null) {
         return false;
      }
      if (offer.getFulfillmentChannel() != FulfillmentChannel.KINDLE) {
         return false;
      }
      
      //DOPS has a ton of price/marketplace/delivery fee validation logic....instead....
      IonBool amazonChannelPricesVerified = draft.getBool("amazon_channel_prices_verified");
      if (IonValueUtils.anyNull(amazonChannelPricesVerified) || !amazonChannelPricesVerified.booleanValue()) {
         return false;
      }
         
      return true;
   }

   private static void addDraftScopeAttributes(final DigitalItem item, final Book book,
      final TypeSafeIonStruct draft) {
      book.setPisbn(null);
      item.setProvenance(getStringValue(draft, PROVENANCE));
      IonBool isDRM = draft.getBool(IS_DRM);
      if (!anyNull(isDRM)) {
         book.setIsDRM(isDRM.booleanValue());
      }
      IonString interiorType = draft.getString(INTERIOR_TYPE);
      if (!anyNull(interiorType)) {
         item.setConversionType(ConversionType.fromValue(interiorType.stringValue()));
      }
      book.setCoverCreatorDraftId(getStringValue(draft, DRAFT_COVER_CREATOR_DESIGN_ID));
      book.setCoverCreatorSavedId(getStringValue(draft, SAVED_COVER_CREATOR_DESIGN_ID));
      IonString publisherCoverChoice = draft.getString(PUBLISHER_COVER_CHOICE);
      if (!anyNull(publisherCoverChoice)) {
         if ("COVER_CREATOR".equals(publisherCoverChoice.stringValue())) {
            item.setCoverChoice("cover-creator");
         } else if ("QUICK_COVER".equals(publisherCoverChoice.stringValue())) {
            item.setCoverChoice("quick-cover");
         } else {
            item.setCoverChoice("upload");
         }
      }
      IonBool isBookLending = draft.getBool(IS_BOOK_LENDING);
      if (!anyNull(isBookLending)) {
         if (isBookLending.booleanValue()) {
            item.setLendingPlan(LendingPlan.BASIC);
         } else {
            item.setLendingPlan(LendingPlan.NONE);
         } 
      }
      IonDecimal royaltyRate = draft.getDecimal(ROYALTY_RATE);
      if (!anyNull(royaltyRate)) {
         item.setRoyaltyPlanId(ROYALTY_PLAN_TO_RATE_MAPPING.inverse()
            .get(royaltyRate.doubleValue()));
      }
      book.setEdition(getStringValue(draft, EDITION));
      item.setTitle(getStringValue(draft, TITLE));
      item.setTitlePronunciation(getStringValue(draft, TITLE_PRONUNCIATION));
      item.setTitleRomanized(getStringValue(draft, TITLE_ROMANIZED));
      book.setSubtitle(getStringValue(draft, SUBTITLE));
      book.setSubtitlePronunciation(getStringValue(draft, SUBTITLE_PRONUNCIATION));
      book.setSubtitleRomanized(getStringValue(draft, SUBTITLE_ROMANIZED));
      IonString language = draft.getString(LANGUAGE);
      if (!anyNull(language)) {
         item.setLanguage(lookupLanguage(language.stringValue()));
      }
      book.setSeriesTitle(getStringValue(draft, SERIES_TITLE));
      book.setSeriesTitlePronunciation(getStringValue(draft, SERIES_TITLE_PRONUNCIATION));
      book.setSeriesTitleRomanized(getStringValue(draft, SERIES_TITLE_ROMANIZED));
      if (!anyNull(draft.getInt(SERIES_NUMBER))) {
         book.setSeriesVolume(draft.getInt(SERIES_NUMBER).toString());
      } else {
         book.setSeriesVolume(getStringValue(draft, SERIES_VOLUME));
      }
      book.setPublisher(getStringValue(draft, PUBLISHER));
      book.setPublisherRomanized(getStringValue(draft, PUBLISHER_ROMANIZED));
      book.setImprint(getStringValue(draft, IMPRINT));
      book.setPublisherLabel(getStringValue(draft, PUBLISHER_LABEL));
      book.setPublisherLabelPronunciation(getStringValue(draft, PUBLISHER_LABEL_PRONUNCIATION));
      book.setPublisherLabelRomanized(getStringValue(draft, PUBLISHER_LABEL_ROMANIZED));
      addContributorList(item, draft);
      addInternalPageList(item, draft);
      item.setProductDescription(getStringValue(draft, DESCRIPTION));
      IonBool isPublicDomain = draft.getBool(IS_PUBLIC_DOMAIN);
      if (!anyNull(isPublicDomain)) {
         book.setIsPublicDomain(isPublicDomain.booleanValue());
         book.setPublishingRights(isPublicDomain.booleanValue() ?
            PublishingRightsStatus.PUBLIC_DOMAIN : PublishingRightsStatus.NOT_APPLICABLE);
      }
      List<String> keywords = getStringListValue(draft, KEYWORDS, false);
      if (!CollectionUtils.isEmpty(keywords)) {
         item.setKeywords(keywords);
      } else {
         item.setKeywords(new ArrayList<>());
      }
      IonBool isAdultContent = draft.getBool(IS_ADULT_CONTENT);
      if (!anyNull(isAdultContent) && "japanese".equals(item.getLanguage())) {
         if (isAdultContent.booleanValue()) {
            item.setAdultContentType(AdultContentType.APPLICABLE);
         } else {
            item.setAdultContentType(AdultContentType.NOT_APPLICABLE);
         }
      }
      addCategories(draft, item);
      item.setThesaurusSubjectKeywords(new ArrayList<>());
      book.setPageTurnDirection(getStringValue(draft, PAGE_TURN_DIRECTION));
      addReadingInterestAge(draft, item);
      addGradeLevelRange(draft, item);
      String homeMarketplaceDBC = getStringValue(draft, HOME_MARKETPLACE);
      item.setHomeMarketplace("UK".equals(homeMarketplaceDBC) ? "GB" : homeMarketplaceDBC);
      addSalesTerritories(draft, item);

      
      List<StageStatus> stages = new ArrayList<>();
      StageStatus contentStageStatus = getContentStageStatus(draft);
      StageStatus metadataStageStatus = new StageStatus();
      metadataStageStatus.setStageName(StageName.METADATA);
      StageStatus territoryRightsStageStatus = new StageStatus();
      territoryRightsStageStatus.setStageName(StageName.TERRITORY_RIGHTS);
      StageStatus offersStageStatus = new StageStatus();
      offersStageStatus.setStageName(StageName.OFFERS);

      stages.add(metadataStageStatus);
      stages.add(contentStageStatus);
      stages.add(territoryRightsStageStatus);
      stages.add(offersStageStatus);
      
      item.setStages(stages);
   }

   private static StageStatus getContentStageStatus(TypeSafeIonStruct draft) {
      StageStatus contentStageStatus = new StageStatus();
      TypeSafeIonStruct fileProcessingStruct = draft.getStruct("file_processing");
      IonString fileProcessingId = draft.getString("file_processing_id");

      contentStageStatus.setStageName(StageName.CONTENT);
      if (!anyNull(fileProcessingStruct)) {
         IonString fileProcessingStatusIon = fileProcessingStruct.getString("status");
         String fileProcessingStatus = anyNull(fileProcessingStatusIon) ? "IN_PROGRESS" : fileProcessingStatusIon.stringValue();

         if ("COMPLETED".equals(fileProcessingStatus)) {
            contentStageStatus.setStatusCode("FINISHED");
            contentStageStatus.setMessage("CONVERTED");
            contentStageStatus.setStageState(StageState.FINISHED);
         }
         else if ("FAILED".equals(fileProcessingStatus)) {
            IonList statusMessages = fileProcessingStruct.getList("status_messages");

            if (!anyNull(statusMessages) && !statusMessages.isEmpty()) {
               IonStruct messageStruct = ((IonStruct)statusMessages.get(0));
               IonText errorCodeIon = ((IonText)messageStruct.get("error_code"));
               IonText errorMessageIon = ((IonText)messageStruct.get("error_message"));
               String errorCode = anyNull(errorCodeIon) ? null : errorCodeIon.stringValue();
               String errorMessage = anyNull(errorMessageIon) ? null : errorMessageIon.stringValue();

               if (DBC_TO_DOPS_REJECTION_CODE_MAP.containsKey(errorCode)) {
                  contentStageStatus.setStatusCode(DBC_TO_DOPS_REJECTION_CODE_MAP.get(errorCode));
                  contentStageStatus.setMessage(errorMessage);
                  contentStageStatus.setStageState(StageState.REJECTED);
               }
               else {
                  contentStageStatus.setStatusCode(errorCode);
                  contentStageStatus.setMessage(errorMessage);
                  contentStageStatus.setStageState(StageState.ERROR);
               }
            }
            else {
               contentStageStatus.setStatusCode("ConversionError");
               contentStageStatus.setMessage("Conversion error occurred");
               contentStageStatus.setStageState(StageState.ERROR);
            }
         }
         else {
            contentStageStatus.setStatusCode("CONVERTING");
            contentStageStatus.setMessage("INITIATED_CONVERSION");
            contentStageStatus.setStageState(StageState.CONVERTING);
         }
      }
      else if (!anyNull(fileProcessingId)) {
         contentStageStatus.setStatusCode("CONVERTING");
         contentStageStatus.setMessage("INITIATED_CONVERSION");
         contentStageStatus.setStageState(StageState.CONVERTING);
      }
      else {
         contentStageStatus.setStageState(StageState.PROCESSING);
      }

      return contentStageStatus;
   }

   private static void addPreorder(final TypeSafeIonStruct digitalBook, final DigitalItem item, final Book book) {
      TypeSafeIonStruct draft = digitalBook.getStruct(DRAFT_SCOPE);
      TypeSafeIonStruct live = digitalBook.getStruct(LIVE_SCOPE);
      TypeSafeIonStruct state = digitalBook.getStruct(STATE);
      IonTimestamp preorderReleaseDate = anyNull(draft) ? null : draft.getTimestamp(PUBLICATION_DATE);
      
      // Title is a preorder title
      if (!anyNull(preorderReleaseDate)) {
         //Not used:
         //preorder.setPreorderID();
         //preorder.setPreorderStatus();
         //preorder.setStartDate();
         //preorder.setEndDate();
         //preorder.setDatePushReason();
         //livePreorder.setLivePreorderID();
         item.setPublicationDate(preorderReleaseDate.dateValue());
         book.setPublicationDate(preorderReleaseDate.dateValue());
         Preorder preorder = new Preorder();
         preorder.setReleaseDate(preorderReleaseDate.dateValue());
         preorder.setVersion2(true);
         preorder.setFinalizedDate(getPreorderFinalizedDate(digitalBook));
         preorder.setDeleted(false);
         IonInt remainingPushesAllowed = digitalBook.getInt(PREORDER_REMAINING_PUSHES_ALLOWED);
         if (!anyNull(remainingPushesAllowed)) {
            preorder.setRemainingPushesAllowed(remainingPushesAllowed.intValue());
         }
         
         //Preorder has been cancelled after preorder button was available but before book transitioned to live (available for purchase)
         if (!anyNull(state)) {
            IonTimestamp preorderCancelledDate = state.getTimestamp(STATE_PREORDER_CANCELLED_DATE);
            IonString preorderCancelledReason = state.getString(STATE_PREORDER_CANCELLED_REASON);
            if (!anyNull(preorderCancelledReason)) {
               preorder.setCancellationDate(preorderCancelledDate.dateValue());
               preorder.setCancellationReason(preorderCancelledReason.stringValue());
            }
         }
         item.setPreorder(preorder);
         
         if (!anyNull(live)) {
            Date publicationDate = live.getTimestamp(PUBLICATION_DATE).dateValue();
            IonTimestamp preorderLiveDate = state.getTimestamp(STATE_PREORDER_LIVE_DATE);
            if (publicationDate.after(new Date())) {
               //1- Preorder button is live on store but release date has not been reached yet
               addLivePreorder(digitalBook, item, publicationDate);
            } else if (!anyNull(preorderLiveDate)) {
               //2- Book transitioned from preorder to live (available for purchase)
               addLivePreorder(digitalBook, item, preorderLiveDate.dateValue());
            }
         }

      }
   }
   
   private static Date getPreorderFinalizedDate(final TypeSafeIonStruct digitalBook) {
      //CAPA automatically appends 'PST' dropping the offset in the date.  So, returning a date in PST but with UTC offset...
      Date finalizedDate = null;
      TypeSafeIonStruct state = digitalBook.getStruct(STATE);
      if(!anyNull(state)) {
         IonTimestamp preorderFinalizedDate = state.getTimestamp(STATE_REVIEW_START_DATE);
         if(!anyNull(preorderFinalizedDate)) {
            Date preorderFinalizedDateValue = preorderFinalizedDate.dateValue();
            long pstOffset = TimeZone.getTimeZone("PST").getOffset(preorderFinalizedDateValue.getTime());
            finalizedDate = new Date(preorderFinalizedDateValue.getTime() + pstOffset);
         }
      }
      return finalizedDate;
   }

   private static void addLivePreorder(final TypeSafeIonStruct digitalBook, DigitalItem item, Date preorderLiveDate) {
      LivePreorder livePreorder = new LivePreorder();
      livePreorder.setReleaseDate(preorderLiveDate);
      livePreorder.setFinalizedDate(getPreorderFinalizedDate(digitalBook));
      livePreorder.setVersion2(true);
      item.getLiveDigitalItem().setLivePreorder(livePreorder);
   }

   private static void addCategories(final TypeSafeIonStruct draft,
      final DigitalItem item) {
      // tsk list contains null elements in DBC for categories with no tsks
      List<String> thesaurusSubjectKeywords = 
            getStringListValue(draft, THESAURUS_SUBJECT_KEYWORDS, true);
      IonList categoryListIon = draft.getList(CATEGORIES);
      List<Category> categories = new ArrayList<>();
      if (!anyNull(categoryListIon) && !CollectionUtils.isEmpty(categoryListIon)) {
         for (int i = 0; i < categoryListIon.size(); i++) {
            IonValue categoryIon = categoryListIon.get(i);
            String bisacCode = IonTypeCast.toString(categoryIon).stringValue();
            if (!StringUtils.isEmpty(bisacCode)) {
               //Not used:
               //category.setCategoryID();
               //category.setCategoryName();
               // TODO: Tyler should use BisacCategory for all categories
               BisacSubCategory category = new BisacSubCategory();
               category.setBisacCode(bisacCode);
               category.setCategoryName(BISAC_CODE_TO_LITERAL.computeIfAbsent(bisacCode, bisacCodeLookupFunction));
               category.setType("BisacSubCategory");
               if (thesaurusSubjectKeywords != null &&
                  i < thesaurusSubjectKeywords.size() &&
                  thesaurusSubjectKeywords.get(i) != null) {
                  category.setSubCategory(thesaurusSubjectKeywords.get(i));
               }
               categories.add(category);
            }
         }
      }
      item.setCategories(categories);
   }

   private static void addSalesTerritories(final TypeSafeIonStruct draft,
      final DigitalItem item) {
      TypeSafeIonStruct salesTerritoriesIon = draft.getStruct(TERRITORY_RIGHTS);
      Set<SalesTerritory> salesTerritories = new HashSet<>();
      if (!anyNull(salesTerritoriesIon)) {
         IonBool allTerritories = salesTerritoriesIon.getBool("**");
         if (allTerritories != null && allTerritories.booleanValue()) {
            for (String countryCode : SALES_TERRITORIES) {
               salesTerritories.add(getSalesTerritory(countryCode));
            }
         } else {
            for (String countryCode : SALES_TERRITORIES) {
               IonBool territorySelected = salesTerritoriesIon.getBool(countryCode);
               if (!anyNull(territorySelected) && territorySelected.booleanValue()) {
                  salesTerritories.add(getSalesTerritory(countryCode));
               }
            }
         }
      }
      item.setIsSalesRightsAccepted(!salesTerritories.isEmpty());
      item.setSalesTerritories(salesTerritories);
   }

   private static SalesTerritory getSalesTerritory(final String countryCode) {
      SalesTerritory territory = new SalesTerritory();
      //Not used:
      //territory.setCountryCodeID();
      
      territory.setCountryCode(countryCode);
      return territory;
   }

   private static void addGradeLevelRange(final TypeSafeIonStruct draft,
      final DigitalItem item) {
      TypeSafeIonStruct gradeLevelIon = draft.getStruct(GRADE_LEVEL);
      if (!anyNull(gradeLevelIon)) {
         item.setMinGradeLevel(getStringValue(gradeLevelIon, GRADE_LEVEL_MIN));
         item.setMaxGradeLevel(getStringValue(gradeLevelIon, GRADE_LEVEL_MAX));
         if (item.isSetMinGradeLevel() || item.isSetMaxGradeLevel()) {
            item.setGradeLevelType("us_school_grade");
         }
      }
   }

   private static void addReadingInterestAge(final TypeSafeIonStruct draft,
      final DigitalItem item) {
      TypeSafeIonStruct interestAgeIon = draft.getStruct(READING_INTEREST_AGE);
      if (!anyNull(interestAgeIon)) {
         IonInt minAge = interestAgeIon.getInt(READING_INTEREST_AGE_MIN);
         if (!anyNull(minAge)) {
            item.setMinAge(minAge.intValue());
         }
         IonInt maxAge = interestAgeIon.getInt(READING_INTEREST_AGE_MAX);
         if (!anyNull(maxAge)) {
            item.setMaxAge(maxAge.intValue());
         }
      }
   }

   private static void addContributorList(final DigitalItem item,
      final TypeSafeIonStruct draft) {
      List<Contributor> contributors = new ArrayList<>();

      if (!anyNull(draft.get(PRIMARY_AUTHOR)) || !anyNull(draft.get(CONTRIBUTORS))) {
         TypeSafeIonStruct primaryAuthor = draft.getStruct(PRIMARY_AUTHOR);
         if (!anyNull(primaryAuthor)) {
            addContributor(contributors, primaryAuthor);
         }
         IonList contributorsIon = draft.getList(CONTRIBUTORS);
         if (!anyNull(contributorsIon)) {
            for (IonValue contributorIon : contributorsIon) {
               if (!anyNull(contributorIon)) {
                  addContributor(contributors, IonStructs.newTypeSafeStruct(contributorIon));
               }
            }
         }
      }
      item.setContributors(contributors);
   }

   @VisibleForTesting
   protected static void addInternalPageList(final DigitalItem item, final TypeSafeIonStruct draft) {
      if (anyNull(draft.get(INTERNAL_PAGES))) {
         return;
      }

      List<InternalPage> internalPages = new ArrayList<>();
      IonList internalPageListIon = draft.getList(INTERNAL_PAGES);
      for (IonValue ionValue : internalPageListIon) {
         if (!anyNull(ionValue)) {
            TypeSafeIonStruct internalPageIon = IonStructs.newTypeSafeStruct(ionValue);
            InternalPage internalPage = new InternalPage();
            internalPage.setId(getStringValue(internalPageIon, INTERNAL_PAGE_ID));
            internalPage.setVersion(internalPageIon.getInt(INTERNAL_PAGE_VERSION).intValue());
            internalPage.setAssetType(getStringValue(internalPageIon, INTERNAL_PAGE_ASSET_TYPE));
            internalPages.add(internalPage);
         }
      }
      item.setInternalPages(internalPages);
   }

   private static void addContributor(final List<Contributor> contributors,
      final TypeSafeIonStruct struct) {
      //Not used:
      //contributor.setContributorID();
      //contributor.setFullName();
      
      Contributor contributor = new Contributor();
      IonString roleCode = struct.getString(CONTRIBUTOR_ROLE_CODE);
      if (!anyNull(roleCode)) {
         contributor.setType(ContributorType.fromValue(
            KDP_CONTRIBUTOR_ROLE_CODES.inverse().get(roleCode.stringValue())));
      }
      
      String firstName = getStringValue(struct, CONTRIBUTOR_FIRST_NAME);
      String lastName = getStringValue(struct, CONTRIBUTOR_LAST_NAME);
      
      boolean hasFirstName = StringUtils.isNotBlank(firstName);
      boolean hasLastName = StringUtils.isNotBlank(lastName);
      if(hasFirstName && hasLastName) {
         contributor.setName(firstName);
         contributor.setLastName(lastName);
      }
      else if(hasFirstName) {
         contributor.setName(firstName);
      }
      else if(hasLastName) {
         contributor.setName(lastName);
      }
      
      if (hasFirstName || hasLastName) {
         contributor.setFullName((hasLastName ? lastName : "") + ", " + (hasFirstName ? firstName : ""));
      }
      
      contributor.setContributorPronunciation(getStringValue(struct, CONTRIBUTOR_PRONUNCIATION));
      contributor.setContributorRomanized(getStringValue(struct, CONTRIBUTOR_ROMANIZED));
      contributors.add(contributor);
   }

   private static void addAutoLitListing(final DigitalItem item,
      final TypeSafeIonStruct matchbook) {
      TypeSafeIonStruct usMatchbook = matchbook.getStruct(MATCHBOOK_MARKETPLACE);
      List<AutoLitListing> autoLitListings = new ArrayList<>();
      if (!anyNull(usMatchbook)) {
         IonDecimal matchbookPrice = usMatchbook.getDecimal(MATCHBOOK_PRICE);
         if (!anyNull(matchbookPrice)) {
            //Not used:
            //matchbookListing.setAutoLitListingId();
            AutoLitListing matchbookListing = new AutoLitListing();
            
            matchbookListing.setChannel("US");
            matchbookListing.setEnrolled(true);
            matchbookListing.setPrice(
               (long) (matchbookPrice.doubleValue() * MATCHBOOK_TO_CENTS_FACTOR));
            
            autoLitListings.add(matchbookListing);
         }
      }
      item.setAutoLitListings(autoLitListings);
   }

   private static void addDigitalOffer(final TypeSafeIonStruct digitalBook,
      final DigitalItem item) {
      List<DigitalOffer> offers = new ArrayList<>(); 
      TypeSafeIonStruct amazonChannelIon = anyNull(digitalBook.getStruct(DRAFT_SCOPE)) ? null : digitalBook.getStruct(DRAFT_SCOPE).getStruct(AMAZON_CHANNEL);
      if (!anyNull(digitalBook.get(ASIN)) ||
         !anyNull(amazonChannelIon)) {
         //Not used:
         //offer.setCostInformation();
         //offer.setDigitalOfferID();
         //offer.setOfferStatus();
         
         DigitalOffer offer = new DigitalOffer();
         if (!anyNull(digitalBook.get(ASIN))) {
            offer.setAsin(digitalBook.getString(ASIN).stringValue());
         }
         offer.setFulfillmentChannel(FulfillmentChannel.KINDLE);
         offer.setLocale(Locale.US);
         if (!anyNull(amazonChannelIon)) {
            addChargeMethods(digitalBook, offer);
         }
         if (!anyNull(digitalBook.get(ITEM_STATUS))) {
            String itemStatus = digitalBook.getString(ITEM_STATUS).stringValue();

            switch (itemStatus) {
               case "live":
               case "live_with_changes":
               case "live_review":
               case "live_publishing":
                  offer.setOfferStatus(OfferStatus.CURRENTLY_OFFERED);
                  break;
               case "live_unpublished":
                  offer.setOfferStatus(OfferStatus.NOT_OFFERED);
                  break;
               case "draft_blocked":
               case "live_blocked":
                  offer.setOfferStatus(OfferStatus.ADMIN_BLOCKED);
                  break;
               default:
                  offer.setOfferStatus(OfferStatus.NEVER_OFFERED);
                  break;
            }
         }
         offers.add(offer);
      }
      item.setCurrentOffers(offers);
   }
   
   private static void addChargeMethods(final TypeSafeIonStruct digitalBook,
      final DigitalOffer offer) {
      List<ChargeMethod> chargeMethods = new ArrayList<>();
      TypeSafeIonStruct amazonChannelIon = 
         digitalBook.getStruct(DRAFT_SCOPE).getStruct(AMAZON_CHANNEL);
      IonString homeMarketplace = 
         digitalBook.getStruct(DRAFT_SCOPE).getString(HOME_MARKETPLACE);
      for (String marketplace : MARKETPLACE_CURRENCY_CODES.keySet()) {
         TypeSafeIonStruct marketplaceIon = amazonChannelIon.getStruct(marketplace);
         if (!anyNull(marketplaceIon)) {
            ChargeMethod chargeMethod = new ChargeMethod();
            //Not used:
            //chargeMethod.setChargeMethodID();
            //chargeMethod.setEndDate();
            //chargeMethod.setIsPriceLocked();
            //chargeMethod.setPriceLockReasonCode();
            //chargeMethod.setPriceUnlockDate();
            //chargeMethod.setStartDate();
            
            IonDecimal price = marketplaceIon.getDecimal(PRICE_VAT_EXCLUSIVE);
            if (price != null) {
               chargeMethod.setPrice(price.doubleValue());
            }
            IonDecimal priceWithTax = marketplaceIon.getDecimal(PRICE_VAT_INCLUSIVE);
            // Marketplaces with no VAT do not have priceWithTax set by DOPS on returned value
            if (!anyNull(priceWithTax) && VAT_MARKETPLACES.contains(marketplace)) {
               chargeMethod.setPriceWithTax(priceWithTax.doubleValue());
            }
            chargeMethod.setType("US".equals(marketplace) ? ChargeMethodType.GLOBAL :
               ChargeMethodType.fromValue(marketplace));
            IonBool converted = marketplaceIon.getBool(CONVERTED);
            boolean convertedDefaulted = anyNull(converted) ? false : converted.booleanValue();
            chargeMethod.setIsConvertedPrice(convertedDefaulted);
            chargeMethod.setPriceType(convertedDefaulted ? PriceType.AUTOMATIC : PriceType.MANUAL);
            if (!anyNull(homeMarketplace) && marketplace.equals(homeMarketplace.stringValue())) {
              chargeMethod.setPriceType(PriceType.BASE);
            }
            chargeMethods.add(chargeMethod);
         }
      }
      offer.setChargeMethods(chargeMethods);
   }
   
   private static Date getPreorderReleaseDate(DigitalItem digitalItem, boolean draft) {
      if (digitalItem.getLiveDigitalItem() != null && digitalItem.getLiveDigitalItem().getLivePreorder() != null && !draft) {
         return digitalItem.getLiveDigitalItem().getLivePreorder().getReleaseDate();
      }
      if (digitalItem.getPreorder() != null && !digitalItem.getPreorder().isDeleted()) {
         return digitalItem.getPreorder().getReleaseDate();
      }
      return null;
   }

   public static IonValue removeSentinel() {
      return ION_SYSTEM.singleValue("(void)");
   }
}