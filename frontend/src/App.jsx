import React, { useEffect, useState } from "react";
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import PlayerRadarChart from "./radarChart";
import ComparePlayersPage from './comparePage';

const API_BASE = "http://localhost:8000";

function App() {
  const [tournaments, setTournaments] = useState([]);
  const [selectedTournament, setSelectedTournament] = useState(null);
  const [players, setPlayers] = useState([]);
  const [selectedPlayer, setSelectedPlayer] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE}/tournaments`)
      .then((res) => res.json())
      .then((data) => setTournaments(data.tournaments));
  }, []);

  useEffect(() => {
    if (selectedTournament) {
      fetch(`${API_BASE}/tournament/${encodeURIComponent(selectedTournament)}`)
        .then((res) => res.json())
        .then((data) =>  setPlayers(data.players));
    }
  }, [selectedTournament]);

  const playerNames = [...new Set(players.map((p) => p.Player))];

  const MainContent = () => (
    <div style={{ 
      padding: "20px",
      maxWidth: "1200px",
      margin: "0 auto",
      fontFamily: "Arial, sans-serif"
    }}>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        marginBottom: "30px"
      }}>
        <h1 style={{ color: "#333" }}>LoL Stats Viewer</h1>
        <Link 
          to="/compare"
          style={{
            padding: "8px 16px",
            backgroundColor: "#8884d8",
            color: "white",
            textDecoration: "none",
            borderRadius: "4px",
            fontSize: "14px"
          }}
        >
          Compare Players
        </Link>
      </div>

      <div style={{ 
        display: "flex", 
        flexDirection: "column", 
        gap: "15px",
        marginBottom: "30px" 
      }}>
        <div style={{ textAlign: "center" }}>
          <label style={{ marginRight: "10px", fontWeight: "bold" }}>
            Select Tournament:
          </label>
          <select
            style={{
              padding: "8px 15px",
              borderRadius: "4px",
              border: "1px solid #ccc",
              fontSize: "16px"
            }}
            value={selectedTournament}
            onChange={(e) => {
              setSelectedTournament(e.target.value);
              setSelectedPlayer(null);
            }}
          >
            <option value="">-- Choose --</option>
            {tournaments.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>

        {playerNames.length > 0 && (
          <div style={{ textAlign: "center" }}>
            <label style={{ marginRight: "10px", fontWeight: "bold" }}>
              Select Player:
            </label>
            <select
              style={{
                padding: "8px 15px",
                borderRadius: "4px",
                border: "1px solid #ccc",
                fontSize: "16px"
              }}
              value={selectedPlayer?.Player || ""}
              onChange={(e) =>
                setSelectedPlayer(
                  players.find((p) => p.Player === e.target.value)
                )
              }
            >
              <option value="">-- Choose Player --</option>
              {playerNames.map((name) => (
                <option key={name} value={name}>{name}</option>
              ))}
            </select>
          </div>
        )}
      </div>

      {selectedPlayer && (
        <div>
          <h2 style={{ textAlign: "center", color: "#444" }}>
            {selectedPlayer.Player}
          </h2>
          <div style={{ 
            textAlign: "center", 
            marginBottom: "20px",
            fontSize: "18px" 
          }}>
            <span style={{ marginRight: "20px" }}>
              Games: {selectedPlayer.Games}
            </span>
            <span>KDA: {selectedPlayer.KDA}</span>
          </div>
          
          <div style={{ 
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "60px", // Increased gap
            padding: "20px",
            maxWidth: "90%", // Added max width
            margin: "0 auto" // Center the grid
          }}>
            <div style={{ 
              backgroundColor: "#fafafa",
              borderRadius: "8px",
              padding: "30px", // Increased padding
              boxShadow: "0 2px 4px rgba(0,0,0,0.05)"
            }}>
              <PlayerRadarChart player={selectedPlayer} />
            </div>
            
            <div style={{ 
              backgroundColor: "#fafafa",
              borderRadius: "8px",
              padding: "30px", // Increased padding
              boxShadow: "0 2px 4px rgba(0,0,0,0.05)"
            }}>
              <table style={{ 
                width: "100%",
                borderCollapse: "collapse"
              }}>
                <tbody>
                  {Object.entries(selectedPlayer)
                    .filter(([key]) => !["Player", "Country"].includes(key))
                    .map(([key, value]) => (
                      <tr key={key} style={{ 
                        borderBottom: "1px solid #eee"
                      }}>
                        <td style={{ 
                          padding: "8px",
                          fontWeight: "bold",
                          color: "#333" // Changed from #666 to #333 for darker text
                        }}>{key}</td>
                        <td style={{ 
                          padding: "8px",
                          textAlign: "right",
                          color: "#333" // Added black color for values
                        }}>{typeof value === 'number' ? value.toFixed(2) : value}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );

  return (
    <Router>
      <Routes>
        <Route path="/" element={<MainContent />} />
        <Route path="/compare" element={<ComparePlayersPage />} />
      </Routes>
    </Router>
  );
}

export default App;
