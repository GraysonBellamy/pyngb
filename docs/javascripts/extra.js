// pyngb documentation enhancements
document.addEventListener('DOMContentLoaded', function() {
  console.debug('🚀 pyngb documentation loaded');

  // Add copy buttons to code blocks without them
  addCopyButtons();

  // Add performance indicators
  addPerformanceIndicators();

  // Smooth scroll for anchor links
  enableSmoothScroll();

  // Add installation progress indicator
  addInstallationProgress();
});

function addCopyButtons() {
  // Find code blocks that don't already have copy buttons
  const codeBlocks = document.querySelectorAll('pre code');

  codeBlocks.forEach(block => {
    const pre = block.parentElement;
    if (pre && !pre.querySelector('.copy-button')) {
      const button = document.createElement('button');
      button.className = 'copy-button';
      button.innerHTML = '📋 Copy';
      button.style.cssText = `
        position: absolute;
        top: 8px;
        right: 8px;
        background: var(--pyngb-accent);
        color: white;
        border: none;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.8rem;
        cursor: pointer;
        opacity: 0.8;
        transition: opacity 0.2s;
      `;

      button.onclick = () => {
        navigator.clipboard.writeText(block.textContent);
        button.innerHTML = '✅ Copied!';
        setTimeout(() => button.innerHTML = '📋 Copy', 2000);
      };

      pre.style.position = 'relative';
      pre.appendChild(button);
    }
  });
}

function addPerformanceIndicators() {
  // Add visual indicators for performance claims
  const performanceText = document.body.innerHTML;
  if (performanceText.includes('0.1-1 sec/file')) {
    const indicators = document.querySelectorAll('td:contains("~0.5 seconds")');
    indicators.forEach(cell => {
      cell.style.background = 'linear-gradient(90deg, #4caf50, #81c784)';
      cell.style.color = 'white';
      cell.style.fontWeight = 'bold';
    });
  }
}

function enableSmoothScroll() {
  // Smooth scroll for internal anchor links
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
      e.preventDefault();
      const target = document.querySelector(this.getAttribute('href'));
      if (target) {
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
}

function addInstallationProgress() {
  // Add visual feedback for installation command
  const installCode = document.querySelector('code:contains("pip install pyngb")');
  if (installCode) {
    const container = installCode.closest('.codehilite, .highlight');
    if (container) {
      container.style.borderLeft = '4px solid var(--pyngb-success)';

      // Add install hint
      const hint = document.createElement('div');
      hint.innerHTML = '💡 <strong>Pro tip:</strong> Use <code>pip install pyngb[visualization]</code> for plotting support';
      hint.style.cssText = `
        margin-top: 8px;
        padding: 12px;
        background: rgba(76, 175, 80, 0.1);
        border-radius: 4px;
        border-left: 3px solid var(--pyngb-success);
        font-size: 0.9rem;
      `;
      container.parentNode.insertBefore(hint, container.nextSibling);
    }
  }
}

// Add "Back to top" button
window.addEventListener('scroll', function() {
  let scrolled = window.pageYOffset;
  let rate = scrolled * -0.5;

  if (scrolled > 300) {
    if (!document.querySelector('.back-to-top')) {
      const backToTop = document.createElement('button');
      backToTop.className = 'back-to-top';
      backToTop.innerHTML = '⬆️';
      backToTop.style.cssText = `
        position: fixed;
        bottom: 20px;
        right: 20px;
        background: var(--pyngb-accent);
        color: white;
        border: none;
        border-radius: 50%;
        width: 50px;
        height: 50px;
        font-size: 20px;
        cursor: pointer;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transition: all 0.3s ease;
        z-index: 1000;
      `;

      backToTop.onclick = () => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
      };

      document.body.appendChild(backToTop);
    }
  }
});
