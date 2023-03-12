import React, { useState, useEffect } from 'react';
const { MongoClient } = require("mongodb");

const SeatBox = ({ seat }) => {
  const colorClass = seat.occupied ? 'grey' : 'green';
  return (
    <div type={`seat ${colorClass}`}>
      {seat.seat_number}
    </div>
  );
};

const SeatMonitor = () => {
  const [seats, setSeats] = useState([]);

  useEffect(() => {
    const fetchSeats = async () => {
      const client = new MongoClient("mongodb+srv://chenkah@uci.edu:ilovemongo@cluster0.hksonbt.mongodb.net/?retryWrites=true&w=majority");
      await client.connect();
      const db = client.db();
      const seatsData = await db.collection('seat_map').find().toArray();
      setSeats(seatsData);
    };

    const intervalId = setInterval(() => {
      fetchSeats();
    }, 1000);

    return () => {
      clearInterval(intervalId);
    };
  }, []);

  return (
    <div type="seat-grid">
      {seats.map(seat => (
        <SeatBox key={seat.seat_number} seat={seat} />
      ))}
    </div>
  );
};

export default SeatMonitor;