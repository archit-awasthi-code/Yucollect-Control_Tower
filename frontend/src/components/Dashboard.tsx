import React, { useEffect, useState } from 'react';
import { Chart as ChartJS, ArcElement, Tooltip, Legend, CategoryScale, LinearScale, PointElement, LineElement, Title } from 'chart.js';
import { Pie, Line } from 'react-chartjs-2';

ChartJS.register(
  ArcElement,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title
);

interface AllocationStats {
  status_counts: Array<{ status: string; count: number }>;
  trend_data: Array<{ date: string; status: string; count: number }>;
  allocator_stats: Array<{ allocator_id: string; total_allocations: number; fully_allocated: number }>;
}

const Dashboard: React.FC = () => {
  const [stats, setStats] = useState<AllocationStats | null>(null);

  useEffect(() => {
    fetchStats();
  }, []);

  const fetchStats = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/allocation-stats');
      const data = await response.json();
      setStats(data);
    } catch (error) {
      console.error('Error fetching stats:', error);
    }
  };

  const pieChartData = {
    labels: stats?.status_counts.map(item => item.status) || [],
    datasets: [
      {
        data: stats?.status_counts.map(item => item.count) || [],
        backgroundColor: [
          '#FF6384',
          '#36A2EB',
          '#FFCE56'
        ],
      },
    ],
  };

  const trendData = {
    labels: stats?.trend_data.map(item => item.date) || [],
    datasets: [
      {
        label: 'Allocations Over Time',
        data: stats?.trend_data.map(item => item.count) || [],
        fill: false,
        borderColor: 'rgb(75, 192, 192)',
        tension: 0.1,
      },
    ],
  };

  return (
    <div className="dashboard">
      <h1>Control Tower Dashboard</h1>
      
      <div className="stats-container">
        <div className="chart-container">
          <h2>Allocation Status Distribution</h2>
          {stats && <Pie data={pieChartData} />}
        </div>
        
        <div className="chart-container">
          <h2>Allocation Trend (Last 7 Days)</h2>
          {stats && <Line data={trendData} />}
        </div>
        
        <div className="allocator-stats">
          <h2>Top Allocators</h2>
          <table>
            <thead>
              <tr>
                <th>Allocator ID</th>
                <th>Total Allocations</th>
                <th>Fully Allocated</th>
                <th>Success Rate</th>
              </tr>
            </thead>
            <tbody>
              {stats?.allocator_stats.map(allocator => (
                <tr key={allocator.allocator_id}>
                  <td>{allocator.allocator_id}</td>
                  <td>{allocator.total_allocations}</td>
                  <td>{allocator.fully_allocated}</td>
                  <td>
                    {((allocator.fully_allocated / allocator.total_allocations) * 100).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
