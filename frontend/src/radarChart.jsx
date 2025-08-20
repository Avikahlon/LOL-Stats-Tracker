import React, { PureComponent } from 'react';
import { Radar, RadarChart, PolarGrid, PolarAngleAxis, 
    PolarRadiusAxis, ResponsiveContainer, Tooltip, 
    Legend} from 'recharts';

export default function PlayerRadarChart({ player }) {
  if (!player) return null;

  const data = [
    { subject: "KDA", value: Math.min(Number(player.KDA) || 0, 100), originalValue: Number(player.KDA) || 0 },
    { subject: "CSM", value: Math.min(Number(player.CSM) || 0, 100), originalValue: Number(player.CSM) || 0 },
    { subject: "GPM", value: Math.min(Number(player.GPM) || 0, 100), originalValue: Number(player.GPM) || 0 },
    { subject: "KP", value: Math.min(Number(player.KP) || 0, 100), originalValue: Number(player.KP) || 0 },
    { subject: "DPM", value: Math.min(Number(player.DPM) || 0, 100), originalValue: Number(player.DPM) || 0 },
    { subject: "GD15", value: Math.min(Math.abs(Number(player.GD15) || 0), 100), originalValue: Number(player.GD15) || 0 },
    { subject: "CSD15", value: Math.min(Math.abs(Number(player.CSD15) || 0), 100), originalValue: Number(player.CSD15) || 0 },
    { subject: "FB", value: Math.min(Math.abs(Number(player["FB "] ) || 0), 100), originalValue: Number(player["FB "] ) || 0 }
  ];

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const { subject, originalValue } = payload[0].payload;
      return (
        <div className="custom-tooltip" style={{ 
          backgroundColor: 'rgba(255, 255, 255, 0.9)',
          padding: '10px',
          border: '1px solid #ccc',
          borderRadius: '4px',
          boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
          color: '#000000'
        }}>
          <p style={{ margin: 0 }}>{`${subject}: ${originalValue.toFixed(2)}`}</p>
        </div>
      );
    }
    return null;
  };

  return (
    <div style={{ width: "100%",
                  height: 400,
                  margin: "0 auto",
                  padding: "20px",}}>
      <h3>Player Performance Radar</h3>
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart cx="50%" cy="50%" outerRadius="80%" data={data}>
          <PolarGrid />
          <PolarAngleAxis dataKey="subject" />
          <PolarRadiusAxis angle={90} domain={[0, 100]} tickCount={5} />
          <Radar
            name={player.Player}
            dataKey="value"
            stroke="#8884d8"
            fill="#8884d8"
            fillOpacity={0.6}
          />
          <Legend />
          <Tooltip content={<CustomTooltip />}/>
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}