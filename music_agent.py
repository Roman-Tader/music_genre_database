#!/usr/bin/env python3
"""
Music Genre Database Agent V4 - Dynamic AI-Powered Generation
Vollständig dynamische Generierung von 15.000+ Musikgenres
"""

import sqlite3
import pandas as pd
import time
import json
import os
import gc
import gzip
import io
import itertools
import concurrent.futures
import hashlib
import yaml
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from threading import Lock
import random
import re

# Für Ähnlichkeitsprüfung (muss installiert werden: pip install jellyfish)
try:
    import jellyfish
except ImportError:
    print("Warning: jellyfish not installed. Duplicate detection limited.")
    jellyfish = None

@dataclass
class Genre:
    """Musikgenre Datenstruktur"""
    id: int
    name: str
    level: int
    parent_id: Optional[int]
    region: str
    language: str
    period: str
    status: str
    instruments: str
    pioneers: str
    artists: str
    works: str
    source: str
    bpm: str  # Neu: BPM Range
    time_signature: str  # Neu: Taktart

class ConfigManager:
    """Verwaltet die Konfiguration für die Genre-Generierung"""
    
    DEFAULT_CONFIG = {
        'generation_rules': {
            'max_genres': 15000,
            'hierarchy_levels': 5,
            'batch_size': 1000,
            'combinations': [
                {
                    'type': 'regional_fusion',
                    'max_output': 2000
                },
                {
                    'type': 'temporal_evolution',
                    'max_output': 1000
                },
                {
                    'type': 'instrumental_variants',
                    'max_output': 1500
                },
                {
                    'type': 'cultural_crossover',
                    'max_output': 2000
                }
            ]
        },
        'validation_rules': {
            'period_format': r'^\d{1,4}(BC)?-(\d{1,4}(AD)?|now)$',
            'artist_format': r'^.+ \(\d{4}-\d{4}\)$',
            'max_name_length': 100,
            'similarity_threshold': 0.85
        },
        'data_sources': {
            'regions': ['USA', 'UK', 'Deutschland', 'Frankreich', 'Italien', 'Spanien',
                       'Japan', 'Korea', 'China', 'Indien', 'Brasilien', 'Mexiko',
                       'Argentinien', 'Südafrika', 'Nigeria', 'Ägypten', 'Australien',
                       'Kanada', 'Russland', 'Türkei', 'Polen', 'Niederlande',
                       'Schweden', 'Norwegen', 'Finnland', 'Island', 'Irland'],
            'eras': ['Prehistoric', 'Ancient', 'Medieval', 'Renaissance', 'Baroque',
                    'Classical', 'Romantic', 'Modern', 'Contemporary', 'Digital'],
            'instruments': ['Guitar/Gitarre', 'Piano/Klavier', 'Drums/Schlagzeug',
                          'Synthesizer', 'Violin/Violine', 'Saxophone/Saxophon',
                          'Trumpet/Trompete', 'Electronic/Elektronisch', 'Traditional'],
            'base_genres': ['Blues', 'Jazz', 'Rock', 'Electronic', 'Hip-Hop',
                          'Classical', 'Folk', 'Country', 'Metal', 'Pop', 'Reggae',
                          'Soul', 'Funk', 'Punk', 'Ambient', 'Experimental']
        }
    }
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = self.DEFAULT_CONFIG.copy()
        if config_file and os.path.exists(config_file):
            self.load_config(config_file)
    
    def load_config(self, config_file: str):
        """Lädt Konfiguration aus YAML-Datei"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                custom_config = yaml.safe_load(f)
                self.config.update(custom_config)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")

class DynamicGenreGenerator:
    """Generiert Genres dynamisch basierend auf Regeln und Mustern"""
    
    def __init__(self, config: ConfigManager):
        self.config = config.config
        self.genre_patterns = {
            'regional_fusion': [
                '{region1} {region2} Fusion',
                '{region1}-{region2} {base} Hybrid',
                'Trans-{region1} {base}',
                '{region1} meets {region2} {base}'
            ],
            'era_genre': [
                '{era} {base}',
                'Neo-{era} {base}',
                '{era} Revival {base}',
                'Post-{era} {base}'
            ],
            'instrument_based': [
                '{instrument} {base}',
                'Electric {instrument} {base}',
                'Acoustic {instrument} {base}',
                '{instrument}-driven {base}'
            ],
            'tempo_variant': [
                'Slow {base}', 'Fast {base}', 'Mid-tempo {base}',
                'Uptempo {base}', 'Downtempo {base}'
            ],
            'mood_genre': [
                'Dark {base}', 'Light {base}', 'Melancholic {base}',
                'Euphoric {base}', 'Aggressive {base}', 'Ambient {base}'
            ],
            'cultural_prefix': [
                'Afro-{base}', 'Indo-{base}', 'Euro-{base}', 'Latin-{base}',
                'Nordic-{base}', 'Oriental-{base}', 'Celtic-{base}'
            ]
        }
        
        # Zusätzliche Modifikatoren für mehr Vielfalt
        self.modifiers = {
            'electronic': ['Synth', 'Digital', 'Cyber', 'Electro', 'Techno'],
            'traditional': ['Folk', 'Traditional', 'Ethnic', 'Native', 'Indigenous'],
            'modern': ['New', 'Modern', 'Contemporary', 'Post', 'Neo'],
            'fusion': ['Fusion', 'Crossover', 'Hybrid', 'Blend', 'Mix']
        }
    
    def generate_combinations(self, limit: int = 15000) -> List[Dict]:
        """Generiert Genre-Kombinationen basierend auf Mustern"""
        generated = []
        regions = self.config['data_sources']['regions']
        eras = self.config['data_sources']['eras']
        instruments = self.config['data_sources']['instruments']
        base_genres = self.config['data_sources']['base_genres']
        
        # 1. Regional Fusions (Cross-Cultural)
        for r1, r2 in itertools.combinations(regions[:15], 2):  # Limitiert für Performance
            for base in base_genres[:10]:
                for pattern in self.genre_patterns['regional_fusion'][:2]:
                    name = pattern.format(region1=r1, region2=r2, base=base)
                    generated.append({
                        'name': name,
                        'type': 'regional_fusion',
                        'components': [r1, r2, base]
                    })
                    if len(generated) >= limit:
                        return generated
        
        # 2. Era-basierte Varianten
        for era in eras:
            for base in base_genres:
                for pattern in self.genre_patterns['era_genre']:
                    name = pattern.format(era=era, base=base)
                    generated.append({
                        'name': name,
                        'type': 'era_variant',
                        'components': [era, base]
                    })
        
        # 3. Instrument-spezifische Genres
        for instrument in instruments[:10]:
            for base in base_genres[:10]:
                for pattern in self.genre_patterns['instrument_based'][:2]:
                    name = pattern.format(
                        instrument=instrument.split('/')[0],
                        base=base
                    )
                    generated.append({
                        'name': name,
                        'type': 'instrument_based',
                        'components': [instrument, base]
                    })
        
        # 4. Subgenre-Varianten mit Modifikatoren
        for base in base_genres:
            # Electronic Varianten
            for mod in self.modifiers['electronic']:
                generated.append({
                    'name': f"{mod} {base}",
                    'type': 'electronic_variant',
                    'components': [mod, base]
                })
            
            # Traditional Varianten
            for mod in self.modifiers['traditional']:
                for region in regions[:10]:
                    generated.append({
                        'name': f"{region} {mod} {base}",
                        'type': 'traditional_variant',
                        'components': [region, mod, base]
                    })
        
        # 5. Mikrogenres durch Mehrfach-Kombination
        for i in range(min(3000, limit - len(generated))):
            # Zufällige Kombination von Elementen
            components = []
            
            # Wähle zufällige Elemente
            if random.random() > 0.5:
                components.append(random.choice(list(self.modifiers['modern'])))
            if random.random() > 0.3:
                components.append(random.choice(regions[:20]))
            if random.random() > 0.4:
                components.append(random.choice(list(self.modifiers['electronic'])))
            
            components.append(random.choice(base_genres))
            
            name = ' '.join(components)
            generated.append({
                'name': name,
                'type': 'micro_genre',
                'components': components
            })
        
        return generated[:limit]

class MetadataEnricher:
    """Reichert Genres mit realistischen Metadaten an"""
    
    def __init__(self):
        self.period_rules = {
            'Ancient': '3000BC-500AD',
            'Medieval': '500-1400',
            'Renaissance': '1400-1600',
            'Baroque': '1600-1750',
            'Classical': '1750-1820',
            'Romantic': '1820-1900',
            'Modern': '1900-2000',
            'Contemporary': '2000-now',
            'Electronic': '1960-now',
            'Digital': '1990-now',
            'Prehistoric': '40000BC-3000BC'
        }
        
        self.instrument_mappings = {
            'Electronic': ['Synthesizer', 'Drum Machine', 'Sampler', 'Computer', 'Sequencer'],
            'Rock': ['Electric Guitar', 'Bass Guitar', 'Drums', 'Vocals'],
            'Jazz': ['Saxophone', 'Trumpet', 'Piano', 'Double Bass', 'Drums', 'Clarinet'],
            'Classical': ['Orchestra', 'Piano', 'Violin', 'Cello', 'Flute', 'Oboe'],
            'Folk': ['Acoustic Guitar', 'Fiddle', 'Harmonica', 'Banjo', 'Mandolin'],
            'Hip-Hop': ['Turntables', 'MPC', 'Vocals', 'Sampler', '808'],
            'Metal': ['Electric Guitar', 'Bass Guitar', 'Double Bass Drum', 'Vocals'],
            'Blues': ['Electric Guitar', 'Acoustic Guitar', 'Harmonica', 'Piano', 'Slide Guitar', 'Vocals'],
            'Country': ['Acoustic Guitar', 'Steel Guitar', 'Fiddle', 'Banjo', 'Vocals'],
            'Reggae': ['Bass Guitar', 'Drums', 'Electric Guitar', 'Keyboards', 'Vocals'],
            'Soul': ['Vocals', 'Electric Guitar', 'Bass', 'Drums', 'Organ', 'Horns'],
            'Funk': ['Bass Guitar', 'Electric Guitar', 'Drums', 'Synthesizer', 'Horns'],
            'Punk': ['Electric Guitar', 'Bass Guitar', 'Drums', 'Vocals'],
            'Ambient': ['Synthesizer', 'Field Recordings', 'Effects', 'Tape Loops'],
            'Pop': ['Vocals', 'Synthesizer', 'Drums', 'Guitar', 'Bass']
        }
        
        self.bpm_mappings = {
            'Ambient': '60-80',
            'Doom Metal': '40-80',
            'Hip-Hop': '85-115',
            'House': '118-135',
            'Techno': '120-150',
            'Drum & Bass': '160-180',
            'Dubstep': '138-142',
            'Trance': '128-140',
            'Blues': '60-120',
            'Jazz': '60-180',
            'Rock': '110-140',
            'Punk': '150-200',
            'Metal': '100-200',
            'Classical': '40-200',
            'Folk': '80-120',
            'Country': '80-120',
            'Reggae': '60-90',
            'Soul': '60-100',
            'Funk': '90-120',
            'Pop': '100-130'
        }
        
        self.time_signature_mappings = {
            'Jazz': ['4/4', '3/4', '5/4', '7/4', '6/8'],
            'Classical': ['4/4', '3/4', '2/4', '6/8', '3/8', '5/4'],
            'Rock': ['4/4', '3/4', '6/8'],
            'Blues': ['4/4', '12/8', '6/8'],
            'Folk': ['4/4', '3/4', '6/8', '2/4'],
            'Electronic': ['4/4', '3/4'],
            'Metal': ['4/4', '3/4', '5/4', '7/8', '9/8'],
            'Punk': ['4/4', '3/4'],
            'Hip-Hop': ['4/4'],
            'Reggae': ['4/4'],
            'Country': ['4/4', '3/4', '2/4'],
            'Pop': ['4/4', '3/4']
        }
        
        self.artist_database = self._generate_artist_database()
    
    def _generate_artist_database(self) -> Dict[str, List[str]]:
        """Generiert eine Datenbank mit plausiblen Künstlernamen"""
        first_names = ['John', 'Maria', 'David', 'Sarah', 'Michael', 'Anna', 'Robert',
                      'Lisa', 'James', 'Emma', 'Carlos', 'Yuki', 'Chen', 'Priya',
                      'Ahmed', 'Olga', 'Jean', 'Hans', 'Giovanni', 'Fatima']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
                     'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Yamamoto', 'Wang',
                     'Singh', 'Ali', 'Ivanov', 'Dubois', 'Schmidt', 'Rossi']
        
        artist_db = {}
        for genre_type in ['Blues', 'Jazz', 'Rock', 'Electronic', 'Classical', 'Folk']:
            artists = []
            for _ in range(50):
                first = random.choice(first_names)
                last = random.choice(last_names)
                # Generiere realistische Lebensdaten basierend auf Genre
                if genre_type == 'Classical':
                    birth = random.randint(1600, 1850)
                    death = birth + random.randint(40, 80)
                elif genre_type in ['Blues', 'Jazz']:
                    birth = random.randint(1880, 1940)
                    death = birth + random.randint(50, 85)
                else:
                    birth = random.randint(1940, 1990)
                    death = 0 if birth > 1960 else birth + random.randint(45, 80)
                
                if death == 0 or death > 2024:
                    artist = f"{first} {last} ({birth}-)"
                else:
                    artist = f"{first} {last} ({birth}-{death})"
                
                artists.append(artist)
            
            artist_db[genre_type] = artists
        
        return artist_db
    
    def generate_realistic_metadata(self, genre_name: str, genre_type: str,
                                  parent_info: Optional[Dict] = None) -> Dict:
        """Generiert kontextbasierte Metadaten für ein Genre"""
        
        # Periode bestimmen
        period = self._infer_period(genre_name)
        
        # Region bestimmen
        region = self._infer_region(genre_name)
        
        # Sprache bestimmen
        language = self._infer_language(region)
        
        # Status bestimmen
        status = self._infer_status(genre_name, period)
        
        # Instrumente bestimmen
        instruments = self._infer_instruments(genre_name)
        
        # BPM bestimmen
        bpm = self._infer_bpm(genre_name)
        
        # Taktart bestimmen
        time_signature = self._infer_time_signature(genre_name)
        
        # Künstler generieren
        pioneers, artists = self._generate_artists(genre_name, period)
        
        # Werke generieren
        works = self._generate_works(genre_name, artists, period)
        
        # Source generieren
        source = self._generate_source(genre_name)
        
        return {
            'region': region[:15],
            'language': language[:5],
            'period': period[:15],
            'status': status,
            'instruments': instruments[:100],
            'pioneers': pioneers[:200],
            'artists': artists[:300],
            'works': works[:300],
            'source': source[:100],
            'bpm': bpm[:15],
            'time_signature': time_signature[:10]
        }
    
    def _infer_period(self, genre_name: str) -> str:
        """Inferiert die Periode aus dem Genre-Namen"""
        for era, period in self.period_rules.items():
            if era.lower() in genre_name.lower():
                return period
        
        # Standard-Perioden basierend auf Genre-Typ
        if any(term in genre_name.lower() for term in ['electronic', 'synth', 'digital', 'cyber']):
            return '1980-now'
        elif any(term in genre_name.lower() for term in ['traditional', 'folk', 'ethnic']):
            return '1800-now'
        elif any(term in genre_name.lower() for term in ['classical', 'baroque', 'romantic']):
            return '1600-1900'
        else:
            return '1950-now'
    
    def _infer_region(self, genre_name: str) -> str:
        """Inferiert die Region aus dem Genre-Namen"""
        # Direkte Länder-/Regionsnamen
        regions = ['USA', 'UK', 'Deutschland', 'France', 'Italia', 'España',
                  'Japan', 'Korea', 'China', 'India', 'Brasil', 'Africa']
        
        for region in regions:
            if region.lower() in genre_name.lower():
                return region
        
        # Regionale Indikatoren
        if any(term in genre_name.lower() for term in ['nordic', 'scandinavian']):
            return 'Scandinavia'
        elif 'latin' in genre_name.lower():
            return 'Latin America'
        elif 'african' in genre_name.lower() or 'afro' in genre_name.lower():
            return 'Africa'
        elif 'asian' in genre_name.lower():
            return 'Asia'
        elif 'european' in genre_name.lower() or 'euro' in genre_name.lower():
            return 'Europe'
        else:
            return 'Global'
    
    def _infer_language(self, region: str) -> str:
        """Inferiert die Sprache basierend auf der Region"""
        language_map = {
            'USA': 'EN', 'UK': 'EN', 'Deutschland': 'DE', 'France': 'FR',
            'Italia': 'IT', 'España': 'ES', 'Japan': 'JA', 'Korea': 'KO',
            'China': 'ZH', 'India': 'HI', 'Brasil': 'PT', 'Russia': 'RU',
            'Global': 'Multi', 'Africa': 'Multi', 'Latin America': 'ES'
        }
        return language_map.get(region, 'EN')
    
    def _infer_status(self, genre_name: str, period: str) -> str:
        """Bestimmt den Status des Genres"""
        if 'extinct' in genre_name.lower() or period.endswith('1900'):
            return 'X'
        elif any(term in genre_name.lower() for term in ['emerging', 'new', 'future']):
            return 'E'
        elif period.endswith('now'):
            return 'A'
        else:
            return 'H'
    
    def _infer_instruments(self, genre_name: str) -> str:
        """Inferiert typische Instrumente aus dem Genre-Namen mit Slash-Trennung"""
        instruments = []
        
        # Check for specific genre types - priorisiere spezifischere Matches
        genre_lower = genre_name.lower()
        matched = False
        
        # Zuerst exakte Genre-Matches
        for genre_type, instr_list in self.instrument_mappings.items():
            if genre_type.lower() in genre_lower:
                instruments.extend(instr_list)
                matched = True
                break
        
        # Wenn kein Match, suche nach verwandten Begriffen
        if not matched:
            if any(term in genre_lower for term in ['guitar', 'string', 'acoustic']):
                instruments.extend(['Acoustic Guitar', 'Electric Guitar'])
            if any(term in genre_lower for term in ['synth', 'electronic', 'digital', 'cyber']):
                instruments.extend(['Synthesizer', 'Drum Machine', 'Sampler'])
            if any(term in genre_lower for term in ['orchestral', 'symphonic', 'chamber']):
                instruments.extend(['Orchestra', 'Strings', 'Woodwinds', 'Brass'])
            if any(term in genre_lower for term in ['vocal', 'a cappella', 'choir']):
                instruments.append('Vocals')
            if any(term in genre_lower for term in ['drum', 'percussion', 'rhythm']):
                instruments.append('Drums')
        
        # Zusätzliche Instrumente basierend auf Keywords
        if 'acoustic' in genre_lower and 'Acoustic Guitar' not in instruments:
            instruments.append('Acoustic Guitar')
        if 'electric' in genre_lower and 'Electric Guitar' not in instruments:
            instruments.append('Electric Guitar')
        if 'piano' in genre_lower and 'Piano' not in instruments:
            instruments.append('Piano')
        if 'sax' in genre_lower and 'Saxophone' not in instruments:
            instruments.append('Saxophone')
        
        # Fallback - verwende Basis-Genre Instrumente
        if not instruments:
            # Versuche Basis-Genre zu identifizieren
            for base in ['rock', 'jazz', 'blues', 'electronic', 'classical', 'folk']:
                if base in genre_lower:
                    instruments.extend(self.instrument_mappings.get(base.capitalize(), ['Various'])[:3])
                    break
        
        # Letzter Fallback
        if not instruments:
            instruments = ['Various']
        
        # Formatiere als Slash-getrennte Liste
        unique_instruments = list(dict.fromkeys(instruments))[:5]  # Max 5 Instrumente, keine Duplikate
        return '/'.join(unique_instruments)
    
    def _generate_artists(self, genre_name: str, period: str) -> Tuple[str, str]:
        """Generiert plausible Künstlernamen basierend auf Genre und Periode mit Slash-Trennung"""
        # Bestimme Basis-Genre für Künstler-Auswahl
        base_genre = 'Rock'  # Default
        for genre in self.artist_database.keys():
            if genre.lower() in genre_name.lower():
                base_genre = genre
                break
        
        available_artists = self.artist_database.get(base_genre, self.artist_database['Rock'])
        
        # Wähle Pioneers (5) und Artists (10)
        pioneers = random.sample(available_artists, min(5, len(available_artists)))
        artists = random.sample(available_artists, min(10, len(available_artists)))
        
        # Formatiere als Slash-getrennte Listen
        return '/'.join(pioneers), '/'.join(artists)
    
    def _generate_works(self, genre_name: str, artists: str, period: str) -> str:
        """Generiert plausible Werktitel mit Slash-Trennung"""
        # Basis-Werktitel
        work_templates = [
            '{adjective} {noun}',
            'The {adjective} {noun}',
            '{noun} in {key}',
            '{verb}ing {noun}',
            '{noun} No. {number}',
            '{place} {noun}',
            '{color} {noun}',
            '{emotion} {noun}'
        ]
        
        adjectives = ['Blue', 'Dark', 'Electric', 'Silent', 'Golden', 'Eternal',
                     'Lost', 'Hidden', 'Sacred', 'Wild']
        nouns = ['Sky', 'Dream', 'River', 'Mountain', 'Heart', 'Soul', 'Night',
                'Dawn', 'Storm', 'Journey']
        verbs = ['Dance', 'Fly', 'Dream', 'Sing', 'Cry', 'Love', 'Fight']
        keys = ['A Minor', 'C Major', 'E Flat', 'D Minor']
        places = ['London', 'Paris', 'Tokyo', 'Memphis', 'Berlin']
        colors = ['Blue', 'Red', 'Black', 'White', 'Purple']
        emotions = ['Happy', 'Sad', 'Angry', 'Peaceful', 'Melancholic']
        
        works = []
        for _ in range(10):
            template = random.choice(work_templates)
            
            # Generiere Jahr basierend auf Periode
            if '-' in period:
                start_year = period.split('-')[0]
                if start_year.isdigit():
                    year = random.randint(int(start_year), 2024)
                else:
                    year = random.randint(1950, 2024)
            else:
                year = random.randint(1950, 2024)
            
            work = template.format(
                adjective=random.choice(adjectives),
                noun=random.choice(nouns),
                verb=random.choice(verbs),
                key=random.choice(keys),
                number=random.randint(1, 9),
                place=random.choice(places),
                color=random.choice(colors),
                emotion=random.choice(emotions)
            )
            
            works.append(f"{work} ({year})")
        
        # Formatiere als Slash-getrennte Liste
        return '/'.join(works[:10])
    
    def _infer_bpm(self, genre_name: str) -> str:
        """Inferiert typische BPM-Range aus dem Genre-Namen"""
        genre_lower = genre_name.lower()
        
        # Direkte Genre-Matches
        for genre_type, bpm_range in self.bpm_mappings.items():
            if genre_type.lower() in genre_lower:
                return bpm_range
        
        # Tempo-basierte Keywords
        if any(term in genre_lower for term in ['slow', 'ballad', 'downtempo', 'chill']):
            return '60-90'
        elif any(term in genre_lower for term in ['fast', 'speed', 'hardcore', 'thrash']):
            return '150-200'
        elif any(term in genre_lower for term in ['mid-tempo', 'moderate']):
            return '90-120'
        elif any(term in genre_lower for term in ['uptempo', 'dance', 'club']):
            return '120-140'
        
        # Spezifische Electronic Subgenres
        if 'drum & bass' in genre_lower or 'd&b' in genre_lower:
            return '160-180'
        elif 'dubstep' in genre_lower:
            return '138-142'
        elif 'trap' in genre_lower:
            return '140-170'
        elif 'breakbeat' in genre_lower:
            return '130-150'
        elif 'garage' in genre_lower:
            return '130-140'
        
        # Default basierend auf Haupt-Kategorie
        if any(term in genre_lower for term in ['electronic', 'techno', 'house']):
            return '120-140'
        elif any(term in genre_lower for term in ['rock', 'pop']):
            return '110-140'
        elif any(term in genre_lower for term in ['jazz', 'blues']):
            return '60-120'
        elif any(term in genre_lower for term in ['classical', 'orchestral']):
            return '40-180'
        else:
            return '80-120'  # Default
    
    def _infer_time_signature(self, genre_name: str) -> str:
        """Inferiert typische Taktart aus dem Genre-Namen"""
        genre_lower = genre_name.lower()
        
        # Direkte Genre-Matches
        for genre_type, signatures in self.time_signature_mappings.items():
            if genre_type.lower() in genre_lower:
                # Wähle die häufigste oder eine passende Taktart
                if 'waltz' in genre_lower or 'triple' in genre_lower:
                    return '3/4'
                elif 'shuffle' in genre_lower or 'swing' in genre_lower:
                    return '12/8'
                elif any(term in genre_lower for term in ['complex', 'progressive', 'math']):
                    # Wähle eine ungewöhnliche Taktart
                    uncommon = [s for s in signatures if s not in ['4/4', '3/4']]
                    return uncommon[0] if uncommon else signatures[0]
                else:
                    return signatures[0]  # Häufigste Taktart
        
        # Spezifische Keywords
        if 'waltz' in genre_lower:
            return '3/4'
        elif any(term in genre_lower for term in ['march', 'polka']):
            return '2/4'
        elif any(term in genre_lower for term in ['shuffle', 'swing', 'blues']):
            return '12/8'
        elif any(term in genre_lower for term in ['odd', 'complex', 'progressive']):
            return random.choice(['5/4', '7/4', '7/8', '9/8'])
        elif 'tango' in genre_lower:
            return '4/4'
        elif 'jig' in genre_lower:
            return '6/8'
        
        # Default
        return '4/4'

class HierarchyBuilder:
    """Baut die hierarchische Struktur der Genres auf"""
    
    def __init__(self):
        self.id_counter = 1
        self.id_lock = Lock()
    
    def get_next_id(self) -> int:
        """Thread-safe ID-Generierung"""
        with self.id_lock:
            current_id = self.id_counter
            self.id_counter += 1
            return current_id
    
    def build_hierarchy(self, genre_combinations: List[Dict],
                       enricher: MetadataEnricher) -> List[Genre]:
        """Baut die komplette Genre-Hierarchie auf"""
        genres = []
        parent_map = {}
        level_map = {}  # Track levels für bessere Hierarchie
        
        # Level 1: Hauptgenres
        main_genres = ['Blues', 'Jazz', 'Rock', 'Electronic', 'Hip-Hop', 'Classical',
                      'Folk', 'Country', 'Metal', 'Pop', 'Reggae', 'Soul', 'Funk',
                      'Punk', 'Ambient', 'Experimental', 'World Music', 'Latin',
                      'R&B', 'Gospel']
        
        for main_genre in main_genres:
            metadata = enricher.generate_realistic_metadata(main_genre, 'main')
            genre = Genre(
                id=self.get_next_id(),
                name=main_genre,
                level=1,
                parent_id=None,
                **metadata
            )
            genres.append(genre)
            parent_map[main_genre] = genre.id
            level_map[genre.id] = 1
        
        # Sortiere Kombinationen nach Komplexität für bessere Level-Zuweisung
        sorted_combos = sorted(genre_combinations, key=lambda x: len(x['components']))
        
        # Level 2-5: Subgenres basierend auf Kombinationen
        for combo in sorted_combos:
            # Bestimme Parent basierend auf Komponenten
            parent_id = self._determine_parent(combo, parent_map, genres)
            level = self._determine_level_advanced(combo, level_map, parent_id)
            
            if level <= 5:  # Max 5 Level
                metadata = enricher.generate_realistic_metadata(
                    combo['name'],
                    combo['type']
                )
                
                genre = Genre(
                    id=self.get_next_id(),
                    name=combo['name'][:100],  # Max Länge
                    level=level,
                    parent_id=parent_id,
                    **metadata
                )
                
                genres.append(genre)
                parent_map[combo['name']] = genre.id
                level_map[genre.id] = level
        
        return genres
    
    def _determine_level_advanced(self, combo: Dict, level_map: Dict,
                                 parent_id: Optional[int]) -> int:
        """Bestimmt das hierarchische Level basierend auf Komplexität"""
        if parent_id is None:
            return 1
        
        parent_level = level_map.get(parent_id, 1)
        
        # Level basierend auf Genre-Typ und Komponenten
        components = combo['components']
        combo_type = combo['type']
        
        # Level 2: Direkte Subgenres
        if combo_type in ['era_variant', 'regional_fusion'] and len(components) <= 2:
            return min(parent_level + 1, 2)
        
        # Level 3: Spezifischere Varianten
        elif combo_type in ['instrument_based', 'electronic_variant'] and len(components) <= 3:
            return min(parent_level + 1, 3)
        
        # Level 4: Regionale/Lokale Varianten
        elif combo_type in ['traditional_variant', 'micro_genre'] and len(components) <= 4:
            return min(parent_level + 2, 4)
        
        # Level 5: Sehr spezifische Mikrogenres
        else:
            return min(parent_level + 2, 5)

class QualityAssurance:
    """Validiert und verbessert die Datenqualität"""
    
    def __init__(self, config: ConfigManager):
        self.config = config.config
        self.validation_rules = self.config['validation_rules']
    
    def validate_entry(self, genre: Genre) -> List[str]:
        """Validiert einen einzelnen Genre-Eintrag"""
        errors = []
        
        # Name Validierung
        if len(genre.name) > self.validation_rules['max_name_length']:
            errors.append(f"Name too long: {len(genre.name)} chars")
        
        # Period Format Validierung
        if not self._validate_period_format(genre.period):
            errors.append(f"Invalid period format: {genre.period}")
        
        # Status Validierung
        if genre.status not in ['A', 'H', 'E', 'X']:
            errors.append(f"Invalid status: {genre.status}")
        
        # Hierarchie Validierung
        if genre.level < 1 or genre.level > 5:
            errors.append(f"Invalid level: {genre.level}")
        
        # Parent ID Validierung
        if genre.level > 1 and genre.parent_id is None:
            errors.append("Missing parent_id for non-root genre")
        
        return errors
    
    def _validate_period_format(self, period: str) -> bool:
        """Validiert das Period-Format"""
        pattern = self.validation_rules['period_format']
        return bool(re.match(pattern, period))
    
    def validate_batch(self, genres: List[Genre]) -> Tuple[List[Genre], List[str]]:
        """Validiert eine Batch von Genres"""
        valid_genres = []
        all_errors = []
        
        for genre in genres:
            errors = self.validate_entry(genre)
            if not errors:
                valid_genres.append(genre)
            else:
                all_errors.extend([f"Genre {genre.id} ({genre.name}): {e}"
                                  for e in errors])
        
        return valid_genres, all_errors

class DuplicateDetector:
    """Erkennt und behandelt Duplikate"""
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.genre_groups = {}  # Gruppiert ähnliche Genres
    
    def merge_duplicates(self, genres: List[Genre]) -> List[Genre]:
        """Findet und merged ähnliche Genres"""
        merged_genres = []
        processed = set()
        
        # Gruppiere ähnliche Genres
        for i, g1 in enumerate(genres):
            if i in processed:
                continue
                
            similar_group = [g1]
            similar_indices = {i}
            
            for j, g2 in enumerate(genres[i+1:], i+1):
                if j not in processed and self._are_similar(g1, g2):
                    similar_group.append(g2)
                    similar_indices.add(j)
            
            # Merge die Gruppe
            if len(similar_group) > 1:
                merged = self._merge_genre_group(similar_group)
                merged_genres.append(merged)
            else:
                merged_genres.append(g1)
            
            processed.update(similar_indices)
        
        # Füge nicht-verarbeitete Genres hinzu
        for i, genre in enumerate(genres):
            if i not in processed:
                merged_genres.append(genre)
        
        return merged_genres
    
    def _merge_genre_group(self, group: List[Genre]) -> Genre:
        """Merged eine Gruppe ähnlicher Genres"""
        # Verwende das erste Genre als Basis
        base = group[0]
        
        # Sammle alle Sources
        all_sources = []
        for g in group:
            all_sources.append(g.source)
        
        # Kombiniere unique sources
        unique_sources = list(dict.fromkeys(all_sources))
        combined_source = ' / '.join(unique_sources[:3])  # Max 3 sources
        
        # Erstelle gemergtes Genre mit kombinierten Sources
        merged = Genre(
            id=base.id,
            name=base.name,
            level=base.level,
            parent_id=base.parent_id,
            region=base.region,
            language=base.language,
            period=base.period,
            status=base.status,
            instruments=base.instruments,
            pioneers=base.pioneers,
            artists=base.artists,
            works=base.works,
            source=combined_source,
            bpm=base.bpm,
            time_signature=base.time_signature
        )
        
        return merged
    
    def _are_similar(self, g1: Genre, g2: Genre) -> bool:
        """Prüft ob zwei Genres ähnlich sind"""
        # Exakter Name-Match
        if g1.name.lower() == g2.name.lower():
            return True
        
        # Ähnlichkeit mit jellyfish wenn verfügbar
        if jellyfish:
            similarity = jellyfish.jaro_winkler(g1.name.lower(), g2.name.lower())
            if similarity > self.similarity_threshold:
                return True
        
        # Prüfe auf Varianten (z.B. "UK Hip-Hop" vs "British Hip-Hop")
        if self._are_variants(g1.name, g2.name):
            return True
        
        return False
    
    def _are_variants(self, name1: str, name2: str) -> bool:
        """Prüft ob zwei Namen Varianten voneinander sind"""
        # Synonyme für Regionen
        synonyms = {
            'uk': 'british',
            'british': 'uk',
            'german': 'deutsch',
            'deutsch': 'german',
            'french': 'français',
            'american': 'us',
            'us': 'american'
        }
        
        words1 = set(name1.lower().split())
        words2 = set(name2.lower().split())
        
        # Ersetze Synonyme und prüfe erneut
        for word in words1.copy():
            if word in synonyms:
                words1.add(synonyms[word])
        
        for word in words2.copy():
            if word in synonyms:
                words2.add(synonyms[word])
        
        # Wenn die Wortmengen sehr ähnlich sind
        common = words1.intersection(words2)
        if len(common) >= len(words1) * 0.7 or len(common) >= len(words2) * 0.7:
            return True
        
        return False

class BatchProcessor:
    """Verarbeitet Genres in Batches für bessere Performance"""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.processed_count = 0
    
    def process_in_batches(self, total_genres: int, generator: DynamicGenreGenerator,
                          enricher: MetadataEnricher, builder: HierarchyBuilder,
                          qa: QualityAssurance) -> List[Genre]:
        """Verarbeitet Genres in Batches"""
        all_genres = []
        
        # Generiere Genre-Kombinationen in Batches
        for batch_start in range(0, total_genres, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total_genres)
            batch_size = batch_end - batch_start
            
            print(f"🔄 Processing batch {batch_start}-{batch_end}...")
            
            # Generiere Kombinationen für diesen Batch
            combinations = generator.generate_combinations(batch_size)
            
            # Baue Hierarchie
            batch_genres = builder.build_hierarchy(combinations, enricher)
            
            # Validiere Batch
            valid_genres, errors = qa.validate_batch(batch_genres)
            
            if errors:
                print(f"⚠️  {len(errors)} validation errors in batch")
            
            all_genres.extend(valid_genres)
            self.processed_count += len(valid_genres)
            
            # Garbage Collection nach jedem Batch
            gc.collect()
        
        return all_genres

class ParallelProcessor:
    """Parallele Verarbeitung für bessere Performance"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
    
    def process_parallel(self, tasks: List[Dict], enricher: MetadataEnricher) -> List[Genre]:
        """Verarbeitet Aufgaben parallel"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for task in tasks:
                future = executor.submit(self._process_task, task, enricher)
                futures.append(future)
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    results.extend(result)
                except Exception as e:
                    print(f"❌ Error in parallel processing: {e}")
        
        return results
    
    def _process_task(self, task: Dict, enricher: MetadataEnricher) -> List[Genre]:
        """Verarbeitet eine einzelne Aufgabe"""
        # Hier würde die spezifische Verarbeitung stattfinden
        # Simplified für dieses Beispiel
        return []

class ExportManager:
    """Verwaltet den Export der Genre-Datenbank"""
    
    def __init__(self):
        self.export_formats = ['csv', 'json', 'sqlite', 'compressed']
    
    def export_to_csv(self, genres: List[Genre], filename: str = "music_genres_15k.csv"):
        """Exportiert als CSV mit Komma-Delimiter und Anführungszeichen"""
        print(f"📝 Exporting {len(genres)} genres to CSV...")
        
        # Konvertiere zu DataFrame
        data = []
        for genre in genres:
            data.append([
                genre.id, genre.name, genre.level, genre.parent_id or '',
                genre.region, genre.language, genre.period, genre.status,
                genre.instruments, genre.pioneers, genre.artists, genre.works,
                genre.source, genre.bpm, genre.time_signature
            ])
        
        df = pd.DataFrame(data, columns=[
            'ID', 'Name', 'Lvl', 'PID', 'Region', 'Lang', 'Period', 'Status',
            'Instr', 'Pioneers', 'Artists', 'Works', 'Source', 'BPM', 'TimeSignature'
        ])
        
        # Speichere als CSV mit Komma und Anführungszeichen
        df.to_csv(filename, sep=',', index=False, encoding='utf-8', quoting=1)  # quoting=1 für QUOTE_ALL
        print(f"✅ Saved to {filename}")
        
        return filename
    
    def export_compressed(self, genres: List[Genre], filename: str = "music_genres_15k.csv.gz"):
        """Exportiert als komprimierte CSV mit Komma-Delimiter"""
        print(f"🗜️  Exporting compressed CSV...")
        
        # Erstelle CSV im Memory
        buffer = io.StringIO()
        
        # Header mit Anführungszeichen
        buffer.write('"ID","Name","Lvl","PID","Region","Lang","Period","Status","Instr","Pioneers","Artists","Works","Source","BPM","TimeSignature"\n')
        
        # Daten
        for genre in genres:
            line = f'"{genre.id}","{genre.name}","{genre.level}","{genre.parent_id or ""}","'
            line += f'{genre.region}","{genre.language}","{genre.period}","{genre.status}","'
            line += f'{genre.instruments}","{genre.pioneers}","{genre.artists}","{genre.works}","{genre.source}","'
            line += f'{genre.bpm}","{genre.time_signature}"\n'
            buffer.write(line)
        
        # Komprimiere
        compressed = gzip.compress(buffer.getvalue().encode('utf-8'))
        
        with open(filename, 'wb') as f:
            f.write(compressed)
        
        original_size = len(buffer.getvalue())
        compressed_size = len(compressed)
        ratio = (1 - compressed_size / original_size) * 100
        
        print(f"✅ Compressed to {filename}")
        print(f"📊 Compression: {original_size:,} → {compressed_size:,} bytes ({ratio:.1f}% reduction)")
        
        return filename
    
    def export_statistics(self, genres: List[Genre], filename: str = "genre_statistics.json"):
        """Exportiert Statistiken über die Genre-Datenbank"""
        stats = {
            'total_genres': len(genres),
            'generation_date': datetime.now().isoformat(),
            'hierarchy_levels': {},
            'regions': {},
            'periods': {},
            'status_distribution': {},
            'top_parent_genres': {},
            'average_name_length': sum(len(g.name) for g in genres) / len(genres) if genres else 0
        }
        
        # Level-Verteilung
        for genre in genres:
            level = f"Level_{genre.level}"
            stats['hierarchy_levels'][level] = stats['hierarchy_levels'].get(level, 0) + 1
            
            # Region-Verteilung
            stats['regions'][genre.region] = stats['regions'].get(genre.region, 0) + 1
            
            # Status-Verteilung
            stats['status_distribution'][genre.status] = stats['status_distribution'].get(genre.status, 0) + 1
            
            # Parent Genres
            if genre.parent_id:
                stats['top_parent_genres'][genre.parent_id] = stats['top_parent_genres'].get(genre.parent_id, 0) + 1
        
        # Sortiere Statistiken
        stats['regions'] = dict(sorted(stats['regions'].items(), key=lambda x: x[1], reverse=True)[:20])
        stats['top_parent_genres'] = dict(sorted(stats['top_parent_genres'].items(), key=lambda x: x[1], reverse=True)[:10])
        
        # Speichere als JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        print(f"📊 Statistics saved to {filename}")
        return stats

class MusicGenreAgentV4:
    """Hauptklasse für den Music Genre Database Agent V4"""
    
    def __init__(self, config_file: Optional[str] = None):
        print("🎵 Music Genre Database Agent V4 - Dynamic AI-Powered")
        print("=" * 60)
        
        # Initialisiere Komponenten
        self.config = ConfigManager(config_file)
        self.generator = DynamicGenreGenerator(self.config)
        self.enricher = MetadataEnricher()
        self.builder = HierarchyBuilder()
        self.qa = QualityAssurance(self.config)
        self.batch_processor = BatchProcessor(
            batch_size=self.config.config['generation_rules']['batch_size']
        )
        self.duplicate_detector = DuplicateDetector()
        self.export_manager = ExportManager()
        
        # Statistiken
        self.stats = {
            'start_time': time.time(),
            'genres_generated': 0,
            'duplicates_removed': 0,
            'validation_errors': 0
        }
    
    def generate_database(self, target_count: int = 15000) -> List[Genre]:
        """Generiert die komplette Genre-Datenbank"""
        print(f"🚀 Generating {target_count:,} music genres...")
        
        # Phase 1: Generierung
        print("\n📋 Phase 1: Genre Generation")
        all_genres = self.batch_processor.process_in_batches(
            target_count,
            self.generator,
            self.enricher,
            self.builder,
            self.qa
        )
        
        self.stats['genres_generated'] = len(all_genres)
        print(f"✅ Generated {len(all_genres):,} genres")
        
        # Phase 2: Duplikaterkennung und Merging
        print("\n🔍 Phase 2: Duplicate Detection & Merging")
        initial_count = len(all_genres)
        all_genres = self.duplicate_detector.merge_duplicates(all_genres)
        self.stats['duplicates_removed'] = initial_count - len(all_genres)
        print(f"✅ Merged {self.stats['duplicates_removed']:,} duplicate genres")
        
        # Phase 3: Finale Validierung
        print("\n✔️  Phase 3: Final Validation")
        valid_genres, errors = self.qa.validate_batch(all_genres)
        self.stats['validation_errors'] = len(errors)
        
        if errors:
            print(f"⚠️  {len(errors)} validation errors found")
            # Log erste 10 Fehler
            for error in errors[:10]:
                print(f"   - {error}")
        
        print(f"✅ {len(valid_genres):,} valid genres ready for export")
        
        return valid_genres
    
    def export_database(self, genres: List[Genre], format: str = 'csv'):
        """Exportiert die Datenbank in verschiedenen Formaten"""
        print(f"\n💾 Phase 4: Export ({format})")
        
        if format == 'csv':
            self.export_manager.export_to_csv(genres)
        elif format == 'compressed':
            self.export_manager.export_compressed(genres)
        elif format == 'both':
            self.export_manager.export_to_csv(genres)
            self.export_manager.export_compressed(genres)
        
        # Exportiere immer Statistiken
        stats = self.export_manager.export_statistics(genres)
        
        return stats
    
    def print_summary(self, genres: List[Genre]):
        """Gibt eine Zusammenfassung aus"""
        elapsed_time = time.time() - self.stats['start_time']
        
        print("\n" + "=" * 60)
        print("📊 GENERATION SUMMARY")
        print("=" * 60)
        print(f"✅ Total Genres Generated: {len(genres):,}")
        print(f"⏱️  Total Time: {elapsed_time:.1f} seconds")
        print(f"🚀 Generation Rate: {len(genres)/elapsed_time:.0f} genres/second")
        print(f"🔍 Duplicates Removed: {self.stats['duplicates_removed']:,}")
        print(f"❌ Validation Errors: {self.stats['validation_errors']:,}")
        
        # Level-Verteilung
        level_dist = {}
        for genre in genres:
            level = f"Level {genre.level}"
            level_dist[level] = level_dist.get(level, 0) + 1
        
        print("\n📈 Hierarchy Distribution:")
        for level, count in sorted(level_dist.items()):
            percentage = (count / len(genres)) * 100
            print(f"   {level}: {count:,} ({percentage:.1f}%)")
        
        # Top Regionen
        region_dist = {}
        for genre in genres:
            region_dist[genre.region] = region_dist.get(genre.region, 0) + 1
        
        print("\n🌍 Top 10 Regions:")
        for region, count in sorted(region_dist.items(), key=lambda x: x[1], reverse=True)[:10]:
            percentage = (count / len(genres)) * 100
            print(f"   {region}: {count:,} ({percentage:.1f}%)")
        
        print("\n✅ Database generation complete!")
        print("📁 Files created:")
        print("   - music_genres_15k.csv")
        print("   - music_genres_15k.csv.gz")
        print("   - genre_statistics.json")
    
    def run(self, target_count: int = 15000, export_format: str = 'both'):
        """Hauptausführung"""
        try:
            # Generiere Datenbank
            genres = self.generate_database(target_count)
            
            # Exportiere Datenbank
            self.export_database(genres, export_format)
            
            # Zeige Zusammenfassung
            self.print_summary(genres)
            
            return genres
            
        except Exception as e:
            print(f"\n❌ Error during generation: {e}")
            import traceback
            traceback.print_exc()
            return []

def main():
    """Hauptfunktion"""
    # Optionale Konfigurationsdatei
    config_file = "genre_config.yaml" if os.path.exists("genre_config.yaml") else None
    
    # Erstelle und starte Agent
    agent = MusicGenreAgentV4(config_file)
    
    # Generiere 15.000+ Genres
    genres = agent.run(target_count=15000, export_format='both')
    
    print(f"\n🎉 Successfully generated {len(genres):,} music genres!")
    print("🎵 The world's most comprehensive music genre database is ready!")

if __name__ == "__main__":
    main()
