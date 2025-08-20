import React, { PureComponent } from 'react';

const Navbar = () => {
  return (

<nav className="navbar">
  <div className='nav-left'>
    <a href="/" className='logo'></a>
  </div>
  <div className='nav-center'>
    <div className='nav-links'>
        <li>Home</li>
        <li>Tournaments</li>
        <li>Players</li>
        <li>Teams</li>
    </div>
  </div>
  <div className='nav-right'>
    <a href="/"></a>
  </div>
</nav>
);
};

export default Navbar;