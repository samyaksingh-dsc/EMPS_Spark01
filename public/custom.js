// Enhanced loading.js with Welcome Animation
window.addEventListener('DOMContentLoaded', function() {
    // Create loading overlay
    const loadingOverlay = document.createElement('div');
    loadingOverlay.id = 'loading-overlay';
    loadingOverlay.innerHTML = `
        <div class="loader-container">
            <div class="logo-wrapper">
                <div class="company-logo">
                    <img src="/public/logo_light.png" alt="EM Spark Logo" />
                </div>
                <div class="spinner-circle"></div>
                <div class="spinner-circle-secondary"></div>
            </div>
            <div class="loading-text">Loading EM-SPARK</div>
            <div class="loading-subtext">Energyminds Power Solutions - Smart Power Analytics & Resource Knowledge</div>
        </div>
    `;
    document.body.appendChild(loadingOverlay);

    // Remove loading overlay after 2 seconds and show welcome screen
    setTimeout(function() {
        loadingOverlay.style.opacity = '0';
        setTimeout(function() {
            loadingOverlay.remove();
            showWelcomeAnimation();
        }, 500);
    }, 2000);
});

// Welcome animation function
function showWelcomeAnimation() {
    const welcomeScreen = document.createElement('div');
    welcomeScreen.id = 'welcome-screen';
    welcomeScreen.innerHTML = `
        <div class="welcome-container">
            <div class="welcome-logo">
                <img src="/public/logolight.png" alt="EM Spark" class="welcome-logo-img" />
            </div>
            
            <h1 class="welcome-title">Welcome to <span class="em-spark">EM Spark</span></h1>
            
            <p class="welcome-subtitle">Power Derivative Market Intelligence</p>
            
            <div class="welcome-cards">
                <div class="welcome-card card-1">
                    <div class="card-icon">⭐</div>
                    <div class="card-text"><strong>MCX & NSE Futures</strong><br><small>Real-time derivative analysis</small></div>
                </div>
                <div class="welcome-card card-2">
                    <div class="card-icon">📊</div>
                    <div class="card-text"><strong>DAM/GDAM Data</strong><br><small>Spot market reference</small></div>
                </div>
                <div class="welcome-card card-3">
                    <div class="card-icon">🔍</div>
                    <div class="card-text"><strong>Market Intelligence</strong><br><small>Comprehensive insights</small></div>
                </div>
            </div>
            
            <div class="welcome-features">
                <p class="feature-text">✨ Start by asking:</p>
                <ul class="feature-list">
                    <li>"MCX power September 2025"</li>
                    <li>"DAM price for 15/08/2025"</li>
                    <li>"Average GDAM August 2025"</li>
                </ul>
            </div>
            
            <button class="welcome-button">Enter SPARK</button>
        </div>
    `;
    
    document.body.appendChild(welcomeScreen);
    
    // Animate in
    setTimeout(() => {
        welcomeScreen.classList.add('active');
    }, 100);
    
    // Close welcome screen on button click
    document.querySelector('.welcome-button').addEventListener('click', function() {
        welcomeScreen.style.opacity = '0';
        setTimeout(() => {
            welcomeScreen.remove();
        }, 500);
    });
    
    // Auto close after 6 seconds
    setTimeout(() => {
        if (document.getElementById('welcome-screen')) {
            welcomeScreen.style.opacity = '0';
            setTimeout(() => {
                if (document.getElementById('welcome-screen')) {
                    welcomeScreen.remove();
                }
            }, 500);
        }
    }, 6000);
}

// Create floating disclaimer button
window.addEventListener('load', function() {
    setTimeout(function() {
        // Hide the default sidebar
        const sidebar = document.querySelector('.sidebar, #sidebar, [class*="sidebar"]');
        if (sidebar) {
            sidebar.style.display = 'none';
        }
        
        // Create floating disclaimer button
        const disclaimerBtn = document.createElement('button');
        disclaimerBtn.id = 'floating-disclaimer-btn';
        disclaimerBtn.innerHTML = `
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10"></circle>
                <line x1="12" y1="16" x2="12" y2="12"></line>
                <line x1="12" y1="8" x2="12.01" y2="8"></line>
            </svg>
            <span>Disclaimer</span>
        `;
        document.body.appendChild(disclaimerBtn);
        
        // Create disclaimer modal
        const disclaimerModal = document.createElement('div');
        disclaimerModal.id = 'disclaimer-modal';
        disclaimerModal.innerHTML = `
            <div class="disclaimer-content">
                <button class="close-disclaimer">&times;</button>
                <h2>⚠️ Disclaimer</h2>
                <div class="disclaimer-body">
                    <p><strong>Primary Service:</strong> MCX/NSE Derivative Market Analysis</p>
                    <p><strong>Complementary:</strong> DAM/GDAM/RTM Spot Data</p>
                    <hr>
                    <h3>Important Notice</h3>
                    <ul>
                        <li>This platform provides market intelligence for <strong>research and analysis purposes only</strong></li>
                        <li>Spot market data (DAM/GDAM/RTM) is supplementary reference information</li>
                        <li><strong>Not financial advice</strong> — Consult licensed professionals before trading</li>
                        <li>Data provided "as is" without warranties</li>
                        <li>Energy Minds is not liable for trading decisions</li>
                    </ul>
                    <hr>
                    <h3>📊 Data Information</h3>
                    <ul>
                        <li><strong>Date Range:</strong> 01/04/2022 to 31/10/2025</li>
                        <li><strong>Granularity:</strong> Hourly & 15-min slots</li>
                        <li><strong>Source:</strong> IEX website, MCX, and NSE Website</li>
                        <li><strong>Status:</strong> Development phase</li>
                    </ul>
                    <hr>
                    <p style="text-align: center; font-size: 12px; color: #6b7280; margin-top: 20px;">
                        © 2025 Energy Minds Power Solutions Pvt. Ltd.
                    </p>
                </div>
            </div>
        `;
        document.body.appendChild(disclaimerModal);
        
        // Toggle disclaimer modal
        disclaimerBtn.addEventListener('click', function() {
            disclaimerModal.style.display = 'flex';
        });
        
        document.querySelector('.close-disclaimer').addEventListener('click', function() {
            disclaimerModal.style.display = 'none';
        });
        
        disclaimerModal.addEventListener('click', function(e) {
            if (e.target === disclaimerModal) {
                disclaimerModal.style.display = 'none';
            }
        });
    }, 2000);
});


// Centered "empty chat" like ChatGPT, then revert to normal after 1st message.
(function () {
  const $ = (s, r = document) => r.querySelector(s);

  let originalParent = null;
  let originalNext = null;

  function getComposerWrap() {
    return (
      $('[data-testid="composer-wrapper"]') ||
      $('.cl__composer-wrap') ||
      $('footer')?.parentElement ||
      $('[class*="composer"]')?.parentElement
    );
  }

  function getMessageList() {
    return (
      $('[data-testid="message-list"]') ||
      $('.cl__messages') ||
      $('[class*="MessageList"]') ||
      $('[class*="messages"]')
    );
  }

  function isChatEmpty() {
    const list = getMessageList();
    if (!list) return true;
    if (list.children.length === 0) return true;
    return !(
      list.querySelector('[data-testid="message"]') ||
      list.querySelector('.cl__message') ||
      list.querySelector('article')
    );
  }

  function ensureOverlay() {
    let overlay = $('#cl-empty-overlay');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'cl-empty-overlay';
      const box = document.createElement('div');
      box.id = 'cl-empty-box';
      const h = document.createElement('div');
      h.id = 'cl-empty-title';
      h.textContent = 'What are you working on?';
      box.appendChild(h);
      overlay.appendChild(box);
      document.body.appendChild(overlay);
    }
    return overlay;
  }

  function enterEmpty() {
    const wrap = getComposerWrap();
    if (!wrap) return;
    if (!originalParent) {
      originalParent = wrap.parentElement;
      originalNext = wrap.nextSibling; // can be null
    }
    const overlay = ensureOverlay();
    const box = $('#cl-empty-box');
    if (wrap.parentElement !== box) box.appendChild(wrap);
    document.body.classList.add('cl-empty');
  }

  function exitEmpty() {
    const wrap = getComposerWrap();
    if (!wrap || !originalParent) return;
    if (wrap.parentElement && wrap.parentElement.id === 'cl-empty-box') {
      originalParent.insertBefore(wrap, originalNext);
    }
    document.body.classList.remove('cl-empty');
    const overlay = $('#cl-empty-overlay');
    if (overlay) overlay.remove();
  }

  function reevaluate() {
    if (isChatEmpty()) enterEmpty();
    else exitEmpty();
  }

  function init() {
    reevaluate();

    // Watch for first message, new chat, etc.
    const mo = new MutationObserver(reevaluate);
    mo.observe(document.body, { childList: true, subtree: true });
    window.addEventListener('hashchange', reevaluate);
    window.addEventListener('popstate', reevaluate);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();



