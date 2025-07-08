import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, 
    PolarRadiusAxis, ResponsiveContainer, Tooltip, 
    Legend} from 'recharts';

const API_BASE = "http://localhost:8000";

const CustomTooltip = ({ active, payload }) => {
  if (active && payload && payload.length >= 2) {
    const { subject, originalA, originalB } = payload[0].payload;
    const player1Name = payload[0].name;
    const player2Name = payload[1].name;

    return (
      <div className="custom-tooltip" style={{ 
        backgroundColor: 'rgba(255, 255, 255, 0.95)',
        padding: '12px',
        border: '1px solid #ccc',
        borderRadius: '4px',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
        color: '#000000',
        
      }}>
        <p style={{ margin: '0 0 8px 0', fontWeight: 'bold' }}>{subject}</p>
        <p style={{ margin: '0', color: '#8884d8' }}>
          {`${player1Name}: ${originalA.toFixed(2)}`}
        </p>
        <p style={{ margin: '0', color: '#82ca9d' }}>
          {`${player2Name}: ${originalB.toFixed(2)}`}
        </p>
      </div>
    );
  }
  return null;
};

function ComparePlayersPage() {
  const [tournaments, setTournaments] = useState([]);
  const [players1, setPlayers1] = useState([]);
  const [players2, setPlayers2] = useState([]);
  const [selectedTournament1, setSelectedTournament1] = useState(null);
  const [selectedTournament2, setSelectedTournament2] = useState(null);
  const [selectedPlayer1, setSelectedPlayer1] = useState(null);
  const [selectedPlayer2, setSelectedPlayer2] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE}/tournaments`)
      .then((res) => res.json())
      .then((data) => setTournaments(data.tournaments));
  }, []);

  useEffect(() => {
    if (selectedTournament1) {
      fetch(`${API_BASE}/tournament/${encodeURIComponent(selectedTournament1)}`)
        .then((res) => res.json())
        .then((data) => setPlayers1(data.players));
    }
  }, [selectedTournament1]);

  useEffect(() => {
    if (selectedTournament2) {
      fetch(`${API_BASE}/tournament/${encodeURIComponent(selectedTournament2)}`)
        .then((res) => res.json())
        .then((data) => setPlayers2(data.players));
    }
  }, [selectedTournament2]);

  const PlayerSelector = ({ 
    tournaments,
    selectedTournament,
    setSelectedTournament,
    players,
    selectedPlayer,
    setSelectedPlayer,
    label
  }) => (
    <div style={{ 
      backgroundColor: "#fafafa",
      borderRadius: "8px",
      padding: "20px",
      boxShadow: "0 2px 4px rgba(0,0,0,0.05)",
      margin: "0 auto",
      width: "90%",
      maxWidth: "600px"
    }}>
      <h3 style={{ marginBottom: "15px" }}>{label}</h3>
      <div style={{ marginBottom: "15px" }}>
        <label style={{ marginRight: "10px", fontWeight: "bold" }}>Tournament: </label>
        <select
          style={{
            padding: "8px 15px",
            borderRadius: "4px",
            border: "1px solid #ccc",
            fontSize: "16px"
          }}
          value={selectedTournament || ""}
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

      {players.length > 0 && (
        <div>
          <label style={{ marginRight: "10px", fontWeight: "bold" }}>Player: </label>
          <select
            style={{
              padding: "8px 15px",
              borderRadius: "4px",
              border: "1px solid #ccc",
              fontSize: "16px"
            }}
            value={selectedPlayer?.Player || ""}
            onChange={(e) => setSelectedPlayer(players.find(p => p.Player === e.target.value))}
          >
            <option value="">-- Choose Player --</option>
            {players.map((p) => (
              <option key={p.Player} value={p.Player}>{p.Player}</option>
            ))}
          </select>
        </div>
      )}
    </div>
  );

  return (
    <div className="compare-page-container" style={{ 
      padding: "20px",
      margin: "0 auto",
      fontFamily: "Arial, sans-serif",
      
    }}>
      <div className="header-container" style={{ 
        display: "flex", 
        justifyContent: "space-between", 
        alignItems: "center",
        marginBottom: "30px",
        width: "100vw",
      }}>
        <h1 style={{ color: "#333" }}>Compare Players</h1>
        <Link className='back-link'
          to="/"
          style={{
            padding: "8px 16px",
            backgroundColor: "#8884d8",
            color: "white",
            textDecoration: "none",
            borderRadius: "4px",
            fontSize: "14px"
          }}
        >
          Back to Single View
        </Link>
      </div>

      <div className="players-grid-container" style={{ 
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: "40px",
        marginBottom: "40px",
        width: "100%",
        margin: "0 auto"
      }}>
        <PlayerSelector
          tournaments={tournaments}
          selectedTournament={selectedTournament1}
          setSelectedTournament={setSelectedTournament1}
          players={players1}
          selectedPlayer={selectedPlayer1}
          setSelectedPlayer={setSelectedPlayer1}
          label="Player 1"
        />
        <PlayerSelector
          tournaments={tournaments}
          selectedTournament={selectedTournament2}
          setSelectedTournament={setSelectedTournament2}
          players={players2}
          selectedPlayer={selectedPlayer2}
          setSelectedPlayer={setSelectedPlayer2}
          label="Player 2"
        />
        {selectedPlayer1 && selectedPlayer2 && (
          <>
            {(() => {
              const data = [
                { 
                  subject: "KDA", 
                  A: Math.min(Number(selectedPlayer1.KDA) || 0, 100),
                  B: Math.min(Number(selectedPlayer2.KDA) || 0, 100),
                  originalA: Number(selectedPlayer1.KDA) || 0,
                  originalB: Number(selectedPlayer2.KDA) || 0
                },
                { 
                  subject: "CSM", 
                  A: Math.min(Number(selectedPlayer1.CSM) || 0, 100),
                  B: Math.min(Number(selectedPlayer2.CSM) || 0, 100),
                  originalA: Number(selectedPlayer1.CSM) || 0,
                  originalB: Number(selectedPlayer2.CSM) || 0
                },
                { 
                  subject: "GPM", 
                  A: Math.min(Number(selectedPlayer1.GPM) || 0, 100),
                  B: Math.min(Number(selectedPlayer2.GPM) || 0, 100),
                  originalA: Number(selectedPlayer1.GPM) || 0,
                  originalB: Number(selectedPlayer2.GPM) || 0
                },
                { 
                  subject: "KP", 
                  A: Math.min(Number(selectedPlayer1.KP) || 0, 100),
                  B: Math.min(Number(selectedPlayer2.KP) || 0, 100),
                  originalA: Number(selectedPlayer1.KP) || 0,
                  originalB: Number(selectedPlayer2.KP) || 0
                },
                { 
                  subject: "DPM", 
                  A: Math.min(Number(selectedPlayer1.DPM) || 0, 100),
                  B: Math.min(Number(selectedPlayer2.DPM) || 0, 100),
                  originalA: Number(selectedPlayer1.DPM) || 0,
                  originalB: Number(selectedPlayer2.DPM) || 0
                },
                { 
                  subject: "GD15", 
                  A: Math.min(Math.abs(Number(selectedPlayer1.GD15) || 0), 100),
                  B: Math.min(Math.abs(Number(selectedPlayer2.GD15) || 0), 100),
                  originalA: Number(selectedPlayer1.GD15) || 0,
                  originalB: Number(selectedPlayer2.GD15) || 0
                },
                { 
                  subject: "CSD15", 
                  A: Math.min(Math.abs(Number(selectedPlayer1.CSD15) || 0), 100),
                  B: Math.min(Math.abs(Number(selectedPlayer2.CSD15) || 0), 100),
                  originalA: Number(selectedPlayer1.CSD15) || 0,
                  originalB: Number(selectedPlayer2.CSD15) || 0
                },
                { 
                  subject: "FB", 
                  A: Math.min(Math.abs(Number(selectedPlayer1["FB "]) || 0), 100),
                  B: Math.min(Math.abs(Number(selectedPlayer2["FB "]) || 0), 100),
                  originalA: Number(selectedPlayer1["FB "]) || 0,
                  originalB: Number(selectedPlayer2["FB "]) || 0
                }
              ];

              return (
                <div className="radar-chart-container" style={{
                  backgroundColor: "white",
                  padding: "20px",
                  borderRadius: "8px",
                  boxShadow: "0 2px 4px rgba(0,0,0,0.05)",
                  width: "100%",
                  maxWidth: "800px", 
                  margin: "0 auto",   
                  height: "600px"
                }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <RadarChart cx="50%" cy="50%" outerRadius="80%" data={data}>
                      <PolarGrid />
                      <PolarAngleAxis dataKey="subject" />
                      <PolarRadiusAxis angle={30} domain={[0, 100]} tickCount={5} />
                      <Radar 
                        name={selectedPlayer1.Player} 
                        dataKey="A" 
                        stroke="#8884d8" 
                        fill="#8884d8" 
                        fillOpacity={0.6} 
                      />
                      <Radar 
                        name={selectedPlayer2.Player} 
                        dataKey="B" 
                        stroke="#82ca9d" 
                        fill="#82ca9d" 
                        fillOpacity={0.6} 
                      />
                      <Legend />
                      <Tooltip content={<CustomTooltip />} />
                    </RadarChart>
                  </ResponsiveContainer>
                </div>
              );
            })()}
          </>
        )}
      </div>
    </div>
  );
}

export default ComparePlayersPage;