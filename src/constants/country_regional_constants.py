class CountryConstant:
    def __init__(self):
        # EUROPE
        self.northern_europe = {'northern_europe': ["Denmark",
                                                    "Estonia",
                                                    "Finland",
                                                    "Iceland",
                                                    "Ireland",
                                                    "Latvia",
                                                    "Lithuania",
                                                    "Norway",
                                                    "Sweden",
                                                    "United Kingdom",
                                                    'England',
                                                    "Faroe Islands",
                                                    ]}

        self.eastern_europe = {'eastern_europe': ["Belarus",
                                                  "Bulgaria",
                                                  "Czechia",
                                                  "Hungary",
                                                  "Moldova",
                                                  "Poland",
                                                  'Turkey',
                                                  "Romania",
                                                  "Russia",
                                                  "Slovakia",
                                                  "Ukraine"]}

        self.southern_europe = {'southern_europe': ["Albania",
                                                    "Andorra",
                                                    "Bosnia and Herzegovina",
                                                    "Croatia",
                                                    "Gibraltar",
                                                    "Greece",
                                                    "Holy See",
                                                    "Italy",
                                                    'Macedonia',
                                                    'North Macedonia',
                                                    "Malta",
                                                    'Montenegro',
                                                    "Portugal",
                                                    'San Marino',
                                                    'Serbia',
                                                    'Slovenia',
                                                    'Spain']}

        self.western_europe = {"western_europe": ['Austria',
                                                  'Belgium',
                                                  'France',
                                                  'Germany',
                                                  'Liechtenstein',
                                                  'Luxembourg',
                                                  'Monaco',
                                                  'Netherlands',
                                                  'Switzerland']}

        self.russia = {'russia': ['Russia']}

        self.europe = {
            'europe': self.northern_europe.get('northern_europe')  + self.eastern_europe.get('eastern_europe')
                    + self.western_europe.get('western_europe') + self.southern_europe.get('southern_europe') + self.russia.get('russia')
        }

        # OCEANIA
        self.oceania = {'oceania': ['Australia',
                                    'New Zealand',
                                    'Fiji',
                                    'New Caledonia',
                                    'Papua New Guinea',
                                    'Solomon Islands',
                                    'Vanuatu',
                                    'American Samoa',
                                    'Cook Islands',
                                    'French Polynesia',
                                    'Niue',
                                    'Samoa',
                                    'Tokelau',
                                    'Tonga',
                                    'Tuvalu',
                                    'Wallis and Futuna Islands',
                                    'Guam',
                                    'Kiribati',
                                    'Marshall Islands',
                                    'Micronesia',
                                    'Nauru',
                                    'Northern Mariana Islands',
                                    'Palau'
                                    ]}

        # ASIA
        self.china = {'china': ['China', 'Taiwan', 'Mongolia', 'Hong Kong']}
        self.japan = {'japan': ['Japan']}
        self.south_korea = {'south_korea': ['South Korea']}

        self.east_asia = {
            'east_asia': self.china.get('china') + self.japan.get('japan') + self.south_korea.get('south_korea')
        }
        self.india = {'india': ['India']}

        self.southern_asia = {'southern_asia': ['Afghanistan',
                                                'Bangladesh',
                                                'Bhutan',
                                                'Iran',
                                                'Maldives',
                                                'Nepal',
                                                'Pakistan',
                                                'Sri Lanka']}


        self.central_asia = {"central_asia": [
            'Tajikistan',
            'Turkmenistan',
            'Uzbekistan',
            'Kyrgyzstan',
            'Kazakhstan',
        ]}


        self.south_asia = {
            'south_asia': self.india.get('india') + self.southern_asia.get('southern_asia') + self.central_asia.get('central_asia')
        }

        self.indochina = {'indochina': [
            'Vietnam',
            'Laos',
            'Cambodia',
            'Myanmar',
            'Thailand',
        ]}

        self.malay = {'malay': [
            'Indonesia',
            'Malaysia',
            'Brunei Darussalam',
            'Singapore',
            'Timor-Leste',
            'Philippines'
        ]}

        self.southeast_asia = {
            'southeast_asia': self.indochina.get('indochina') + self.malay.get('malay')
        }

        self.western_asia = {'western_asia': [
            'Armenia',
            'Azerbaijan',
            'Bahrain',
            'Cyprus',
            'Georgia',
            'Iraq',
            'Israel',
            'Jordan',
            'Kuwait',
            'Lebanon',
            'Oman',
            'Qatar',
            'Saudi Arabia',
            'Palestine',
            'Palestinian Territories',
            'Syria',
            'Turkey',
            'Türkiye',
            'United Arab Emirates',
            'Yemen',
        ]}


        # AFRICA
        self.eastern_africa = {'eastern_africa': ['Burundi',
                                                  'Comoros',
                                                  'Nigeria',
                                                  'Djibouti',
                                                  'Eritrea',
                                                  'Ethiopia',
                                                  'Kenya',
                                                  'Madagascar',
                                                  'Malawi',
                                                  'Mauritius',
                                                  'Mayotte',
                                                  'Mozambique',
                                                  'Reunion',
                                                  'Rwanda',
                                                  'Seychelles',
                                                  'Somalia',
                                                  'South Sudan',
                                                  'Uganda',
                                                  'Tanzania',
                                                  'Zambia',
                                                  'Zimbabwe',
                                                  "Côte d'Ivoire"
                                                  ]}

        self.western_africa = {'western_africa': ['Benin',
                                                  'Burkina Faso',
                                                  'Cabo Verde',
                                                  'Gambia',
                                                  'Ghana',
                                                  'Guinea',
                                                  'Guinea-Bissau',
                                                  'Ivory Coast',
                                                  'Liberia',
                                                  'Mali',
                                                  'Mauritania',
                                                  'Niger',
                                                  'Nigeria',
                                                  'Saint Helena',
                                                  'Senegal',
                                                  'Sierra Leone',
                                                  'Togo']}

        self.northern_africa = {'northern_africa': [
            'Algeria',
            'Egypt',
            'Libya',
            'Morocco',
            'Sudan',
            'Tunisia',
            'Western Sahara'
        ]}

        self.middle_africa = {'middle_africa': [
            'Angola',
            'Cameroon',
            'Central African Republic',
            'Chad',
            'Congo',
            'Democratic Republic of the Congo',
            'Equatorial Guinea',
            'Gabon',
            'Sao Tome and Principe',
        ]}

        self.southern_africa = {'southern_africa': [
            'Botswana',
            'Lesotho',
            'Namibia',
            'South Africa',
            'Eswatini'
        ]}

        self.africa = {
            'africa': self.eastern_africa.get('eastern_africa')
                      + self.middle_africa.get('middle_africa')
                      + self.southern_africa.get('southern_africa')
                      + self.northern_africa.get('northern_africa')
        }

        # AMERICA
        self.united_states = {'united_states': ['United States']}
        self.canada = {'canada': ['Canada']}

        self.caribbean = {'caribbean': [
            'Anguilla',
            'Antigua and Barbuda',
            'Aruba',
            'Bahamas',
            'Barbados',
            'British Virgin Islands',
            'Caribbean Netherlands',
            'Cayman Islands',
            'Cuba',
            'Curaçao',
            'Dominica',
            'Dominican Republic',
            'Grenada',
            'Guadeloupe',
            'Haiti',
            'Jamaica',
            'Martinique',
            'Montserrat',
            'Puerto Rico',
            'Saint Kitts and Nevis',
            'Saint Lucia',
            'Saint Vincent and the Grenadines',
            'Sint Maarten (Dutch part)',
            'Trinidad and Tobago',
            'Turks and Caicos Islands',
            'United States Virgin Islands'
        ]}

        self.central_america = {'central_america': [
            'Mexico',
            'Guatemala',
            'Honduras',
            'Nicaragua',
            'El Salvador',
            'Costa Rica',
            'Panama',
            'Belize',
            'Bermuda'
        ]}

        self.south_america = {'south_america': [
            'Brazil',
            'Colombia',
            'Argentina',
            'Peru',
            'Venezuela',
            'Chile',
            'Ecuador',
            'Bolivia',
            'Paraguay',
            'Uruguay',
            'Guyana',
            'Suriname',
            'French Guiana',
            'Falkland Islands'
        ]}

        self.america = {
            'america': self.central_america.get('central_america') + self.caribbean.get('caribbean')
                        + self.south_america.get('south_america') + self.united_states.get('united_states') + self.canada.get('canada')
        }





        # ALl
        self.regionals = [self.europe, self.africa, self.america, self.south_asia, self.east_asia, self.southeast_asia]

    def get_regional(self, regional):
        return self.__getattribute__(regional)

    def get_regional_of_country(self, country):
        for regional in self.regionals:
            countries = list(regional.values())[0]
            regional_name = list(regional.keys())[0]

            if country in countries:
                return regional_name

        return None

    def get_all_regional(self):
        res = []

        for regional in self.regionals:
            res.append(list(regional.keys())[0])

        return res
