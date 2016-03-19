# NoSQL WPP - Aufgabe 2

Zur Installation/Betrieb sind `redis`, `git`, sowie `nodejs`, `npm` und `bower` notwendig.

Bower ist als npm Packet vorhanden und kann via `npm install -g bower` installiert werden.

## Installation 

```
git clone https://github.com/futjikato/HAW-NoSQL-P2.git
cd HAW-NoSQL-P2
npm install
bower install
```

## Betrieb

Ein Redis Server auf dem default port muss laufen.

Import der Daten: `nodejs import.js plz.data`  
Leeren der DB und Import: `nodejs import -d plz.data`

Web-Interface starten: `nodejs server.js`  
Web-Interface ist verfügabr über Port `8080`
