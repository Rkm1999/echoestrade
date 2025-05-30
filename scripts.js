document.addEventListener('DOMContentLoaded', function() {
    // Global Variables
    const itemSelectorContainer = document.getElementById('item-selector-container');
    const chartCanvas = document.getElementById('priceChart');
    const newSMAPeriodInput = document.getElementById('newSMAPeriod');
    const addSMAButton = document.getElementById('addSMAButton');
    const activeSMAListDiv = document.getElementById('activeSMAList');
    const sidebarSearchInput = document.getElementById('sidebarSearch');
    const chartDisplayTitleElement = document.getElementById('chartDisplayTitle');

    // Timeframe UI Elements
    const btn30D = document.getElementById('btn30D');
    const btn3M = document.getElementById('btn3M');
    const btn6M = document.getElementById('btn6M');
    const btn1Y = document.getElementById('btn1Y');
    const btnAll = document.getElementById('btnAll');
    const btnCustomTimeframe = document.getElementById('btnCustomTimeframe');
    const customDateControls = document.getElementById('customDateControls');
    const customStartDateInput = document.getElementById('customStartDate');
    const customEndDateInput = document.getElementById('customEndDate');
    const applyCustomTimeframeButton = document.getElementById('applyCustomTimeframe');

    const initialViewContentDiv = document.getElementById('initial-view-content');
    const chartViewContentDiv = document.getElementById('chart-view-content');
    const favoriteItemsListDiv = document.getElementById('favorite-items-list'); // For initial view
    const recentlyViewedListDiv = document.getElementById('recently-viewed-list'); // For initial view
    const MAX_RECENT_ITEMS = 10;
    let currentItemPath = null;
    let currentItemName = null;
    let priceChart = null;
    let activeTimeframe = "All"; // "30D", "3M", "6M", "1Y", "All", "Custom"
    let originalPriceData = [];
    let originalLabels = [];
    let activeSMAPeriods = [7]; // Initialize with a default array *first*.
    console.log('Initial activeSMAPeriods:', JSON.stringify(activeSMAPeriods));

    const smaColors = [
        'rgb(255, 99, 132)',  // Pink/Red
        'rgb(54, 162, 235)',  // Blue
        'rgb(255, 206, 86)',  // Yellow
        'rgb(153, 102, 255)', // Purple
        'rgb(255, 159, 64)',  // Orange
        'rgb(46, 204, 113)'   // Green
    ];

    function loadSMAPeriodsFromStorage() {
        let periodsToUse = [7]; // Default value for this function's scope
        const storedSMAPeriods = localStorage.getItem('activeSMAPeriods');
        console.log('Stored SMA periods from localStorage:', storedSMAPeriods);

        if (storedSMAPeriods) {
            try {
                const parsedPeriods = JSON.parse(storedSMAPeriods);
                console.log('Parsed SMA periods:', JSON.stringify(parsedPeriods));
                // Validate: must be an array, and if not empty, all elements must be positive numbers.
                if (Array.isArray(parsedPeriods)) {
                    if (parsedPeriods.length === 0) {
                        periodsToUse = []; // Valid empty array
                    } else if (parsedPeriods.every(p => typeof p === 'number' && p > 0)) {
                        periodsToUse = parsedPeriods; // Valid non-empty array
                    } else {
                        console.warn('Stored SMA periods data is invalid (not all positive numbers). Using default and resetting storage.');
                        localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
                    }
                } else {
                    console.warn('Stored SMA periods data is not an array. Using default and resetting storage.');
                    localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
                }
            } catch (e) {
                console.error('Error parsing stored SMA periods:', e, '. Using default and resetting storage.');
                localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
            }
        } else {
            console.log('No SMA periods found in localStorage. Using default and saving to storage.');
            localStorage.setItem('activeSMAPeriods', JSON.stringify(periodsToUse)); // Save default [7]
        }
        console.log('loadSMAPeriodsFromStorage will return:', JSON.stringify(periodsToUse));
        return periodsToUse;
    }

    activeSMAPeriods = loadSMAPeriodsFromStorage();
    console.log('Active SMA Periods after storage load attempt:', JSON.stringify(activeSMAPeriods));

    function saveSMAPeriods() {
        localStorage.setItem('activeSMAPeriods', JSON.stringify(activeSMAPeriods));
    }

function showChartView() {
    const initialView = document.getElementById('initial-view-content'); // Re-selected
    const chartView = document.getElementById('chart-view-content');   // Re-selected
    // Optional: Add similar console logs here for chartView if needed for further debugging
    // console.log('[Debug] showChartView called.');
    // console.log('[Debug] initialView (re-selected):', initialView);
    // console.log('[Debug] chartView (re-selected):', chartView);
    if (initialView) initialView.style.display = 'none';
    if (chartView) chartView.style.display = 'block';
}

function showInitialView() {
    console.log('[Debug] showInitialView called.');
    const initialView = document.getElementById('initial-view-content'); // Re-selected
    const chartView = document.getElementById('chart-view-content');   // Re-selected
    console.log('[Debug] initialView (re-selected):', initialView);
    console.log('[Debug] chartView (re-selected):', chartView);
    if (chartView) chartView.style.display = 'none';
    if (initialView) {
        initialView.style.display = 'block';
        console.log('[Debug] initialView.style.display set to block');
    }
    refreshInitialViewLists();
}

function getFromLocalStorage(key) {
    const data = localStorage.getItem(key);
    try {
        return data ? JSON.parse(data) : [];
    } catch (e) {
        console.error(`Error parsing ${key} from localStorage:`, e);
        return [];
    }
}

function saveToLocalStorage(key, data) {
    try {
        localStorage.setItem(key, JSON.stringify(data));
    } catch (e) {
        console.error(`Error saving ${key} to localStorage:`, e);
    }
}

function loadFavorites() {
    return getFromLocalStorage('favoriteItems');
}

function loadRecentlyViewed() {
    return getFromLocalStorage('recentlyViewedItems');
}

function isFavorited(itemPath, favorites) { // Changed signature to take itemPath
    if (!itemPath) return false;
    return favorites.some(fav => fav.path === itemPath);
}

function displayItemsForInitialView(items, containerDiv, listName) {
    console.log(`[Debug] displayItemsForInitialView called for ${listName}`, containerDiv, items); // New log
    if (!containerDiv) {
        // console.error(`Container div for "${listName}" not found in initial view.`);
        return;
    }
    containerDiv.innerHTML = ''; // Clear previous items

    if (!items || items.length === 0) {
        console.log(`[Debug] Setting 'No items found' for ${listName}`); // New log
        containerDiv.innerHTML = `<p>No ${listName.toLowerCase()} items found.</p>`;
        return;
    }

    const ul = document.createElement('ul');
    // Note: Styles for this ul/li should be in style.css, adapted from start_style.css
    const currentFavorites = loadFavorites();

    items.forEach(item => {
        if (!item || !item.name || !item.path) {
            console.warn('Skipping invalid item for initial view display:', item);
            return;
        }

        const li = document.createElement('li');

        const itemLinkSpan = document.createElement('span');
        itemLinkSpan.textContent = item.name;
        itemLinkSpan.style.cursor = 'pointer';
        itemLinkSpan.style.textDecoration = 'underline';
        itemLinkSpan.style.color = '#007bff'; // Example style
        itemLinkSpan.addEventListener('click', () => {
            loadItemData(item.path, item.name);
            // loadItemData will call showChartView
        });
        li.appendChild(itemLinkSpan);

        const favButton = document.createElement('button');
        favButton.classList.add('favorite-toggle-initial'); // Distinct class if needed
        const isCurrentlyFavorited = isFavorited(item.path, currentFavorites);
        favButton.textContent = isCurrentlyFavorited ? 'Unfavorite' : 'Favorite';
        favButton.title = isCurrentlyFavorited ? 'Remove from favorites' : 'Add to favorites';
        // Add some basic styling for buttons in JS, or ensure CSS covers it
        favButton.style.marginLeft = '10px';
        favButton.style.padding = '3px 6px';

        favButton.addEventListener('click', () => {
            toggleFavoriteOnItem(item); // Pass the full item object
        });
        li.appendChild(favButton);
        ul.appendChild(li);
    });
    containerDiv.appendChild(ul);
}

function toggleFavoriteOnItem(itemToToggle) {
    if (!itemToToggle || !itemToToggle.path || !itemToToggle.name) {
        console.error('Cannot toggle favorite for invalid item:', itemToToggle);
        return;
    }

    let favorites = loadFavorites();
    const itemIndex = favorites.findIndex(fav => fav.path === itemToToggle.path);

    if (itemIndex > -1) { // Already favorited, so remove
        favorites.splice(itemIndex, 1);
    } else { // Not favorited, so add
        // Ensure we are adding an object with name and path
        favorites.push({ name: itemToToggle.name, path: itemToToggle.path });
    }
    saveToLocalStorage('favoriteItems', favorites);

    refreshInitialViewLists(); // Refresh lists in the initial view

    // If the toggled item is the one currently shown in chart view, update its button too
    if (chartViewContentDiv && chartViewContentDiv.style.display !== 'none' && currentItemPath === itemToToggle.path) {
        updateCurrentItemFavoriteButton();
    }
}

function refreshInitialViewLists() {
    console.log('[Debug] refreshInitialViewLists called.'); // New log
    console.log('[Debug] favoriteItemsListDiv:', favoriteItemsListDiv); // New log
    console.log('[Debug] recentlyViewedListDiv:', recentlyViewedListDiv); // New log
    const favorites = loadFavorites();
    const recentlyViewed = loadRecentlyViewed();
    // Check if the divs exist before trying to display items - they are part of index.html now
    if (favoriteItemsListDiv) {
        displayItemsForInitialView(favorites, favoriteItemsListDiv, 'Favorite');
    }
    if (recentlyViewedListDiv) {
        displayItemsForInitialView(recentlyViewed, recentlyViewedListDiv, 'Recently Viewed');
    }
}

    function filterDataByTimeframe(labels, prices, timeframe, startDateStr, endDateStr) {
        if (!labels || !prices || labels.length === 0 || prices.length === 0) {
            return { filteredLabels: [], filteredPrices: [] };
        }

        if (timeframe === "All") {
            return { filteredLabels: labels.slice(), filteredPrices: prices.slice() };
        }

        const filteredLabels = [];
        const filteredPrices = [];

        // Ensure dates are sorted (originalLabels are assumed to be sorted)
        // Convert latest date string to Date object for calculation
        const latestDataDate = new Date(labels[labels.length - 1] + "T00:00:00Z"); // Use Z for UTC to be safe with date-only strings

        let startDate;
        let endDate = latestDataDate; // Default end date is the latest available date

        if (timeframe === "Custom") {
            if (!startDateStr || !endDateStr) { // Should be validated before calling
                return { filteredLabels: labels.slice(), filteredPrices: prices.slice() }; // Or handle error
            }
            startDate = new Date(startDateStr + "T00:00:00Z");
            endDate = new Date(endDateStr + "T23:59:59Z"); // Inclusive end date
        } else {
            startDate = new Date(latestDataDate);
            switch (timeframe) {
                case "30D":
                    startDate.setDate(startDate.getDate() - 30);
                    break;
                case "3M":
                    startDate.setMonth(startDate.getMonth() - 3);
                    break;
                case "6M":
                    startDate.setMonth(startDate.getMonth() - 6);
                    break;
                case "1Y":
                    startDate.setFullYear(startDate.getFullYear() - 1);
                    break;
                default: // Should not happen if logic is correct
                    return { filteredLabels: labels.slice(), filteredPrices: prices.slice() };
            }
        }
        // Normalize startDate to the beginning of its day for comparison
        startDate.setHours(0, 0, 0, 0);


        for (let i = 0; i < labels.length; i++) {
            const currentDate = new Date(labels[i] + "T00:00:00Z"); // Use Z for UTC
            if (currentDate >= startDate && currentDate <= endDate) {
                filteredLabels.push(labels[i]);
                filteredPrices.push(prices[i]);
            }
        }
        return { filteredLabels, filteredPrices };
    }

    function fetchItemData() {
        fetch('item_data.json')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.json();
            })
            .then(jsonData => {
                itemSelectorContainer.innerHTML = '';
                buildSidebarNavigation(jsonData, itemSelectorContainer, 0);
            })
            .catch(error => {
                console.error('Error fetching item_data.json:', error);
                itemSelectorContainer.innerHTML = '<p>Error loading item data.</p>';
            });
    }

    function buildSidebarNavigation(data, parentElement, level = 0) {
        parentElement.innerHTML = '';

        const ul = document.createElement('ul');
        ul.className = `list-level-${level}`;
        ul.style.listStyleType = 'none';
        ul.style.paddingLeft = level > 0 ? '20px' : '0';

        for (const key in data) {
            const listItem = document.createElement('li');
            listItem.className = 'nav-item';

            const textElement = document.createElement('span');
            textElement.style.cursor = 'pointer';
            listItem.appendChild(textElement);

            if (typeof data[key] === 'string') {
                listItem.classList.add('item-leaf');
                textElement.textContent = key;
                listItem.dataset.csvPath = data[key];
                textElement.addEventListener('click', function() {
                    loadItemData(data[key], key);

                    const currentlySelected = document.querySelector('.nav-item.selected-leaf');
                    if (currentlySelected) {
                        currentlySelected.classList.remove('selected-leaf');
                    }
                    listItem.classList.add('selected-leaf');
                });
            } else if (typeof data[key] === 'object') {
                listItem.classList.add('item-category');
                textElement.textContent = '► ' + key;

                const childListContainer = document.createElement('div');
                childListContainer.className = 'child-list-container';
                childListContainer.style.display = 'none';
                listItem.appendChild(childListContainer);

                textElement.addEventListener('click', function(event) {
                    const isCurrentlyCollapsed = childListContainer.style.display === 'none';
                    if (isCurrentlyCollapsed) {
                        childListContainer.style.display = 'block';
                        textElement.textContent = '▼ ' + key;
                        if (childListContainer.innerHTML.trim() === '') {
                            buildSidebarNavigation(data[key], childListContainer, level + 1);
                        }
                    } else {
                        childListContainer.style.display = 'none';
                        textElement.textContent = '► ' + key;
                    }
                    event.stopPropagation();
                });
            }
            ul.appendChild(listItem);
        }
        parentElement.appendChild(ul);
    }

    function updateActiveTimeframeButton(selectedButton) {
        const timeframeButtons = [btn30D, btn3M, btn6M, btn1Y, btnAll, btnCustomTimeframe];
        timeframeButtons.forEach(button => {
            if (button) { // Ensure button exists
                if (button === selectedButton) {
                    button.classList.add('active');
                } else {
                    button.classList.remove('active');
                }
            }
        });
    }


    function loadItemData(csvPath, itemName) {
        currentItemPath = csvPath;
        currentItemName = itemName;
        console.log("Loading data for:", csvPath, "Item Name:", itemName);

        // Reset timeframe to "All" when new item is loaded
        activeTimeframe = "All";
        if (customDateControls) customDateControls.style.display = 'none';
        updateActiveTimeframeButton(btnAll);

        fetch(csvPath)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status} for ${csvPath}`);
                }
                return response.text();
            })
            .then(csvData => {
                const lines = csvData.trim().split(/\r?\n/);
                if (lines.length <= 1) {
                    console.warn("CSV data has no data rows for", csvPath);
                    originalLabels = [];
                    originalPriceData = [];
                } else {
                    const dataRows = lines.slice(1);
                    const tempLabels = [];
                    const tempPriceData = [];
                    const priceIndex = 2;
                    const dateCreatedIndex = 5;

                    dataRows.forEach(row => {
                        const columns = row.split(',');
                        if (columns.length > Math.max(priceIndex, dateCreatedIndex)) {
                            const rawDate = columns[dateCreatedIndex];
                            tempLabels.push(rawDate.substring(0, 10));
                            const price = parseFloat(columns[priceIndex]);
                            tempPriceData.push(isNaN(price) ? null : price);
                        }
                    });
                    originalLabels = tempLabels;
                    originalPriceData = tempPriceData;
                }

                if (chartDisplayTitleElement && itemName) {
                    chartDisplayTitleElement.textContent = `${itemName} Price Over Time`;
                } else if (chartDisplayTitleElement) {
                    chartDisplayTitleElement.textContent = 'Price Over Time';
                }
                console.log('Loading item data. Current activeSMAPeriods before updateChartWithIndicators:', JSON.stringify(activeSMAPeriods));
                if (originalLabels.length === 0 && originalPriceData.length === 0) {
                    console.warn("No data parsed from CSV:", csvPath, "Clearing chart.");
                    if (priceChart) {
                        priceChart.data.labels = [];
                        priceChart.data.datasets = [];
                        priceChart.update();
                    } else {
                         initializeChart();
                    }
                } else {
                    updateChartWithIndicators();
                }
                // Add to Recently Viewed
                if (itemName && csvPath) {
                    let recentlyViewed = localStorage.getItem('recentlyViewedItems');
                    try {
                        recentlyViewed = recentlyViewed ? JSON.parse(recentlyViewed) : [];
                    } catch (e) {
                        console.error('Error parsing recentlyViewedItems from localStorage:', e);
                        recentlyViewed = [];
                    }

                    const newItem = { name: itemName, path: csvPath };
                    // Remove if already exists to move to top
                    recentlyViewed = recentlyViewed.filter(item => item.path !== csvPath);
                    recentlyViewed.unshift(newItem); // Add to the beginning
                    if (recentlyViewed.length > MAX_RECENT_ITEMS) {
                        recentlyViewed.length = MAX_RECENT_ITEMS; // Trim to max size
                    }
                    localStorage.setItem('recentlyViewedItems', JSON.stringify(recentlyViewed));
                }
                updateCurrentItemFavoriteButton();
            })
            .catch(error => {
                console.error('Failed to load or process item data:', error);
                if (chartDisplayTitleElement) {
                    chartDisplayTitleElement.textContent = 'Failed to Load Item Data';
                }
                 originalLabels = [];
                 originalPriceData = [];
                 if (priceChart) {
                    priceChart.data.labels = [];
                    priceChart.data.datasets = [];
                    priceChart.update();
                    calculateAndDisplayPriceStatistics([], []);
                 } else {
                    initializeChart(); // This already calls calculateAndDisplayPriceStatistics([], [])
                 }
            });
        showChartView();
    }

    function updateCurrentItemFavoriteButton() {
        const favButton = document.getElementById('currentItemFavoriteToggle');
        if (!favButton || !currentItemPath || !currentItemName) {
            if (favButton) favButton.style.display = 'none'; // Hide if no item loaded
            return;
        }
        favButton.style.display = 'inline-block'; // Show if item loaded

        let favorites = localStorage.getItem('favoriteItems');
        try {
            favorites = favorites ? JSON.parse(favorites) : [];
        } catch (e) {
            favorites = [];
        }

        const isFav = favorites.some(fav => fav.path === currentItemPath);
        favButton.textContent = isFav ? 'Remove from Favorites' : 'Add to Favorites';
        favButton.title = isFav ? `Remove ${currentItemName} from favorites` : `Add ${currentItemName} to favorites`;
    }

    function calculateSMA(dates, prices, periodDays) {
        if (!dates || !prices || dates.length !== prices.length || periodDays <= 0) {
          return new Array(prices ? prices.length : 0).fill(null);
        }
        const smaValues = new Array(prices.length).fill(null);
        for (let i = 0; i < dates.length; i++) {
          const currentDateStr = dates[i];
          // Using 'Z' to ensure UTC parsing, consistent with filterDataByTimeframe
          const currentDate = new Date(currentDateStr + "T00:00:00Z");
          const startDate = new Date(currentDate);
          startDate.setDate(currentDate.getDate() - (periodDays - 1));
          let sum = 0;
          let count = 0;
          for (let j = i; j >= 0; j--) {
            const loopDateStr = dates[j];
            // Using 'Z' to ensure UTC parsing
            const loopDate = new Date(loopDateStr + "T00:00:00Z");
            if (loopDate >= startDate && loopDate <= currentDate) {
              if (prices[j] !== null && !isNaN(prices[j])) {
                sum += prices[j];
                count++;
              }
            } else if (loopDate < startDate) {
              break;
            }
          }
          if (count > 0) {
            smaValues[i] = sum / count;
          }
        }
        return smaValues;
    }

    function renderActiveSMAList() {
        if (!Array.isArray(activeSMAPeriods)) {
            console.error('CRITICAL: activeSMAPeriods is not an array in renderActiveSMAList! Reverting to default [7].');
            activeSMAPeriods = [7];
            saveSMAPeriods();
        }
        if (!activeSMAListDiv) return;
        activeSMAListDiv.innerHTML = '';
        if (activeSMAPeriods.length === 0) {
          activeSMAListDiv.textContent = 'No active SMAs.';
          return;
        }
        activeSMAPeriods.forEach(period => {
          const itemDiv = document.createElement('div');
          itemDiv.classList.add('sma-item');
          const periodText = document.createElement('span');
          periodText.textContent = `SMA(${period})`;
          const removeButton = document.createElement('button');
          removeButton.textContent = 'Remove';
          removeButton.dataset.period = period;
          removeButton.addEventListener('click', function() {
            const periodToRemove = parseInt(this.dataset.period, 10);
            activeSMAPeriods = activeSMAPeriods.filter(p => p !== periodToRemove);
            saveSMAPeriods();
            renderActiveSMAList();
            updateChartWithIndicators();
          });
          itemDiv.appendChild(periodText);
          itemDiv.appendChild(removeButton);
          activeSMAListDiv.appendChild(itemDiv);
        });
    }

function calculateAndDisplayPriceStatistics(labels, prices) {
    const statsDisplay = document.getElementById('priceStatsDisplay');
    if (!statsDisplay) {
        console.error('Error: priceStatsDisplay element not found.');
        return;
    }

    if (!prices || prices.length < 1) {
        statsDisplay.innerHTML = '<p>No price data available for the selected period.</p>';
        return;
    }

    const startPrice = prices[0];
    const currentPrice = prices[prices.length - 1];
    const highestPrice = Math.max(...prices.filter(p => p !== null));
    const lowestPrice = Math.min(...prices.filter(p => p !== null));

    function calculatePercentageChange(current, start) {
        if (start === null || start === 0 || current === null) {
            return 'N/A';
        }
        return (((current - start) / start) * 100).toFixed(1);
    }

    let currentPricePercentChange = 'N/A';
    let highestPricePercentChange = 'N/A';
    let lowestPricePercentChange = 'N/A';

    if (prices.length >= 2) { // Only calculate percent changes if there are at least two data points
        currentPricePercentChange = calculatePercentageChange(currentPrice, startPrice);
        highestPricePercentChange = calculatePercentageChange(highestPrice, startPrice);
        lowestPricePercentChange = calculatePercentageChange(lowestPrice, startPrice);
    }

function formatPriceStat(value, percentChange) {
    if (value === null || !isFinite(value)) return 'N/A';

    let priceText = value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' ISK';
    let priceHtml;

    if (percentChange !== 'N/A') {
        const isPositive = parseFloat(percentChange) >= 0;
        const valueClass = isPositive ? 'price-value-positive' : 'price-value-negative';
        priceHtml = `<span class="${valueClass}">${priceText}</span>`;

        const changePrefix = isPositive ? '+' : '';
        const changeClass = isPositive ? 'price-change-positive' : 'price-change-negative';
        priceHtml += ` (<span class="${changeClass}">${changePrefix}${percentChange}%</span>)`;
    } else {
        priceHtml = priceText; // No coloring for price or percentage if percentChange is 'N/A'
    }
    return priceHtml;
}

    let htmlContent = '<ul>';
    htmlContent += `<li>Current Price: ${formatPriceStat(currentPrice, currentPricePercentChange)}</li>`;
    htmlContent += `<li>Highest Price (Period): ${formatPriceStat(highestPrice, highestPricePercentChange)}</li>`;
    htmlContent += `<li>Lowest Price (Period): ${formatPriceStat(lowestPrice, lowestPricePercentChange)}</li>`;
    htmlContent += '</ul>';

    statsDisplay.innerHTML = htmlContent;
}

    if (addSMAButton) {
        addSMAButton.addEventListener('click', function() {
            if (!newSMAPeriodInput) return;
            const newPeriod = parseInt(newSMAPeriodInput.value, 10);
            if (isNaN(newPeriod) || newPeriod < 2 || newPeriod > 200) {
              alert('Please enter a valid SMA period (e.g., 2-200 days).');
              return;
            }
            if (activeSMAPeriods.includes(newPeriod)) {
              alert(`SMA(${newPeriod}) is already active.`);
              return;
            }
            if (activeSMAPeriods.length >= smaColors.length) {
                alert(`Maximum of ${smaColors.length} SMAs can be added.`);
                return;
            }
            activeSMAPeriods.push(newPeriod);
            activeSMAPeriods.sort((a, b) => a - b);
            saveSMAPeriods();
            renderActiveSMAList();
            updateChartWithIndicators();
        });
    }

    function initializeChart() {
        if (!chartCanvas) {
            console.error("Chart canvas not found!");
            return;
        }
        console.log('Initializing chart. Current activeSMAPeriods:', JSON.stringify(activeSMAPeriods));
        const ctx = chartCanvas.getContext('2d');
        if (!ctx) {
            console.error("Failed to get 2D context from canvas!");
            return;
        }

        priceChart = new Chart(ctx, {
          type: 'line',
          data: {
            labels: [],
            datasets: []
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              y: {
                beginAtZero: false,
                ticks: { callback: function(value) { return value.toLocaleString(); } }
              },
              x: {
                type: 'time',
                time: {
                    unit: 'day',
                    tooltipFormat: 'MMM d, yyyy',
                    displayFormats: { day: 'MMM d' }
                },
                ticks: { autoSkip: true, maxTicksLimit: 20 }
              }
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: function(context) {
                    let label = context.dataset.label || '';
                    if (label) { label += ': '; }
                    if (context.parsed.y !== null) { label += context.parsed.y.toLocaleString(); }
                    return label;
                  }
                }
              },
              legend: {
                position: 'top',
              }
            }
          }
        });
        renderActiveSMAList();
        calculateAndDisplayPriceStatistics([], []);
    }

    function updateChartWithIndicators() {
        if (!Array.isArray(activeSMAPeriods)) {
            console.error('CRITICAL: activeSMAPeriods is not an array in updateChartWithIndicators! Reverting to default [7].');
            activeSMAPeriods = [7];
            saveSMAPeriods();
        }
        if (!priceChart) {
            initializeChart();
            if (!priceChart) return;
        }

        // Get custom dates if 'Custom' is active, otherwise they can be null/undefined
        const customStart = (activeTimeframe === 'Custom' && customStartDateInput) ? customStartDateInput.value : null;
        const customEnd = (activeTimeframe === 'Custom' && customEndDateInput) ? customEndDateInput.value : null;

        const { filteredLabels, filteredPrices } = filterDataByTimeframe(
            originalLabels,
            originalPriceData,
            activeTimeframe,
            customStart,
            customEnd
        );

        priceChart.data.labels = filteredLabels; // Use filteredLabels
        const datasets = [];

        if (filteredPrices.length > 0) {
            datasets.push({
              label: 'Item Price',
              data: filteredPrices, // Use filteredPrices
              borderColor: 'rgb(0, 0, 0)',
              backgroundColor: 'rgba(0, 0, 0, 0.1)',
              tension: 0.1,
              borderWidth: 2,
              pointRadius: 2,
              pointHoverRadius: 4,
              order: 0
            });

            activeSMAPeriods.forEach((period, index) => {
                // Pass filteredLabels and filteredPrices to calculateSMA
                const smaData = calculateSMA(filteredLabels, filteredPrices, period);
                const color = smaColors[index % smaColors.length];

                datasets.push({
                    label: `SMA(${period})`,
                    data: smaData,
                    borderColor: color,
                    fill: false,
                    tension: 0.1,
                    borderWidth: 1.5,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                    order: index + 1
                });
            });
        } else {
            datasets.push({
                label: 'Item Price',
                data: [],
                borderColor: 'rgb(0,0,0)'
            });
        }

        priceChart.data.datasets = datasets;
        priceChart.update();
        calculateAndDisplayPriceStatistics(filteredLabels, filteredPrices);
    }

    // Initial calls
    fetchItemData(); // For sidebar
    initializeChart(); // Prepare chart object
    refreshInitialViewLists(); // Prepare initial view data

    // URL Parameter Handling (modified)
    const urlParams = new URLSearchParams(window.location.search);
    const itemPathFromUrl = urlParams.get('itemPath');
    const itemNameFromUrl = urlParams.get('itemName');

    if (itemPathFromUrl && itemNameFromUrl) {
        console.log(`Loading item from URL: ${itemNameFromUrl} (${itemPathFromUrl})`);
        loadItemData(itemPathFromUrl, itemNameFromUrl); // This will also call showChartView
        if (chartDisplayTitleElement && itemNameFromUrl) { // Redundant if loadItemData sets it, but safe
             chartDisplayTitleElement.textContent = `${itemNameFromUrl} Price Over Time`;
        }
    } else {
        showInitialView();
        const favToggleBtn = document.getElementById('currentItemFavoriteToggle');
        if (favToggleBtn) favToggleBtn.style.display = 'none'; // Hide chart's fav button
    }
    // The event listener for currentItemFavoriteToggleBtn should remain as is.
    // The call to updateActiveTimeframeButton(btnAll) can remain.
    // The initial text for chartDisplayTitleElement when no item is selected from sidebar can also remain.

    const showInitialViewBtn = document.getElementById('showInitialViewBtn');
    if (showInitialViewBtn) {
        showInitialViewBtn.addEventListener('click', function() {
            showInitialView();
        });
    }

    // --- Timeframe Button Event Listeners ---
    const predefinedTimeframeButtons = [
        { el: btn30D, timeframe: "30D" },
        { el: btn3M, timeframe: "3M" },
        { el: btn6M, timeframe: "6M" },
        { el: btn1Y, timeframe: "1Y" },
        { el: btnAll, timeframe: "All" }
    ];

    predefinedTimeframeButtons.forEach(item => {
        if (item.el) {
            item.el.addEventListener('click', function() {
                activeTimeframe = item.timeframe;
                updateActiveTimeframeButton(item.el);
                if (customDateControls) customDateControls.style.display = 'none';
                updateChartWithIndicators();
                console.log("Active timeframe set to: " + activeTimeframe);
            });
        }
    });

    if (btnCustomTimeframe) {
        btnCustomTimeframe.addEventListener('click', function() {
            activeTimeframe = "Custom";
            updateActiveTimeframeButton(btnCustomTimeframe);
            if (customDateControls) customDateControls.style.display = 'block';
            // Don't update chart until 'Apply' is clicked for custom dates
            console.log("Active timeframe set to: Custom. Custom controls shown.");
        });
    }

    if (applyCustomTimeframeButton) {
        applyCustomTimeframeButton.addEventListener('click', function() {
            const startDate = customStartDateInput ? customStartDateInput.value : null;
            const endDate = customEndDateInput ? customEndDateInput.value : null;

            if (!startDate || !endDate) {
                alert("Please select both a start and end date for the custom timeframe.");
                return;
            }
            if (new Date(startDate) > new Date(endDate)) {
                alert("Start date cannot be after end date.");
                return;
            }
            activeTimeframe = "Custom"; // Should already be set if custom controls are visible
            updateChartWithIndicators();
            console.log(`Custom timeframe applied: ${startDate} to ${endDate}`);
        });
    }

    function recursiveFilter(ulElement, term) {
        if (!ulElement) return false;

        let anyChildMatch = false;
        const listItems = Array.from(ulElement.children).filter(child => child.tagName === 'LI');

        for (const listItem of listItems) {
            let currentItemMatch = false;
            const textElement = listItem.querySelector('span');
            let itemText = '';
            if (textElement) {
                itemText = textElement.textContent.replace(/^[►▼]\s*/, '').toLowerCase();
                if (itemText.includes(term)) {
                    currentItemMatch = true;
                }
            }

            const childListContainer = listItem.querySelector('.child-list-container');
            let hasMatchingChildren = false;

            if (childListContainer) {
                const childUl = childListContainer.querySelector('ul');
                if (childUl && childUl.children.length > 0) {
                    if (recursiveFilter(childUl, term)) {
                        hasMatchingChildren = true;
                    }
                }
                if (currentItemMatch || hasMatchingChildren) {
                    currentItemMatch = true;
                    childListContainer.style.display = 'block';
                    if (textElement) textElement.textContent = '▼ ' + textElement.textContent.replace(/^[►▼]\s*/, '');
                } else {
                    childListContainer.style.display = 'none';
                    if (textElement) textElement.textContent = '► ' + textElement.textContent.replace(/^[►▼]\s*/, '');
                }
            }

            if (currentItemMatch) {
                listItem.style.display = "";
                anyChildMatch = true;
            } else {
                listItem.style.display = "none";
            }
        }
        return anyChildMatch;
    }

    function resetFilters() {
        const rootList = itemSelectorContainer.querySelector('ul.list-level-0');
        if (!rootList) return;

        const allListItems = rootList.getElementsByTagName('li');
        for (const listItem of allListItems) {
            listItem.style.display = "";
            const textElement = listItem.querySelector('span');
            const childListContainer = listItem.querySelector('.child-list-container');
            if (childListContainer) {
                childListContainer.style.display = 'none';
                if (textElement) textElement.textContent = '► ' + textElement.textContent.replace(/^[►▼]\s*/, '');
            }
        }
    }

    function filterSidebar(searchTerm) {
        const term = searchTerm.toLowerCase();
        const rootList = itemSelectorContainer.querySelector('ul.list-level-0');

        if (!rootList) {
            console.log("Root list not found for filtering.");
            return;
        }

        if (term === "") {
            resetFilters();
            return;
        }
        recursiveFilter(rootList, term);
    }

    if (sidebarSearchInput) {
        sidebarSearchInput.addEventListener('input', function() {
            filterSidebar(this.value);
        });
    }

    console.log("scripts.js loaded and DOMContentLoaded event fired.");

    const currentItemFavoriteToggleBtn = document.getElementById('currentItemFavoriteToggle');
    if (currentItemFavoriteToggleBtn) {
        currentItemFavoriteToggleBtn.addEventListener('click', () => {
            if (!currentItemPath || !currentItemName) {
                alert('No item is currently loaded.');
                return;
            }
            // Call the centralized toggle function
            toggleFavoriteOnItem({ name: currentItemName, path: currentItemPath });
            // toggleFavoriteOnItem will handle localStorage, refreshing initial lists,
            // and updating this button if the current item matches.
        });
    }
    // Note: The URL parameter handling was moved up before this event listener block in the new structure.
    // Ensure this favorite button listener is correctly placed *after* the DOMContentLoaded initial setup.
});
