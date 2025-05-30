document.addEventListener('DOMContentLoaded', () => {
    const favoriteItemsListDiv = document.getElementById('favorite-items-list');
    const recentlyViewedListDiv = document.getElementById('recently-viewed-list');

    const MAX_RECENT_ITEMS = 10; // Max number of recently viewed items to store

    // --- LocalStorage Helper Functions ---
    function getFromLocalStorage(key) {
        const data = localStorage.getItem(key);
        try {
            return data ? JSON.parse(data) : [];
        } catch (e) {
            console.error(`Error parsing ${key} from localStorage:`, e);
            return []; // Return empty array on error
        }
    }

    function saveToLocalStorage(key, data) {
        try {
            localStorage.setItem(key, JSON.stringify(data));
        } catch (e) {
            console.error(`Error saving ${key} to localStorage:`, e);
        }
    }

    // --- Core Logic Functions ---
    function loadFavorites() {
        return getFromLocalStorage('favoriteItems');
    }

    function loadRecentlyViewed() {
        return getFromLocalStorage('recentlyViewedItems');
    }

    function isFavorited(item, favorites) {
        if (!item || !item.path) return false;
        return favorites.some(fav => fav.path === item.path);
    }

    function displayItems(items, containerDiv, listName) {
        if (!containerDiv) {
            console.error(`Container div for "${listName}" not found.`);
            return;
        }
        containerDiv.innerHTML = ''; // Clear previous items

        if (!items || items.length === 0) {
            containerDiv.innerHTML = `<p>No ${listName.toLowerCase()} items found.</p>`;
            return;
        }

        const ul = document.createElement('ul');
        const favorites = loadFavorites(); // Get current favorites to correctly label buttons

        items.forEach(item => {
            if (!item || !item.name || !item.path) {
                console.warn('Skipping invalid item:', item);
                return;
            }

            const li = document.createElement('li');

            const link = document.createElement('a');
            link.href = `index.html?itemPath=${encodeURIComponent(item.path)}&itemName=${encodeURIComponent(item.name)}`;
            link.textContent = item.name;
            li.appendChild(link);

            const favButton = document.createElement('button');
            favButton.classList.add('favorite-toggle');
            favButton.textContent = isFavorited(item, favorites) ? 'Unfavorite' : 'Favorite';
            favButton.title = isFavorited(item, favorites) ? 'Remove from favorites' : 'Add to favorites';
            favButton.addEventListener('click', () => {
                toggleFavorite(item);
            });
            li.appendChild(favButton);

            ul.appendChild(li);
        });
        containerDiv.appendChild(ul);
    }

    function toggleFavorite(itemToToggle) {
        if (!itemToToggle || !itemToToggle.path || !itemToToggle.name) {
            console.error('Cannot toggle favorite for invalid item:', itemToToggle);
            return;
        }

        let favorites = loadFavorites();
        const itemIndex = favorites.findIndex(fav => fav.path === itemToToggle.path);

        if (itemIndex > -1) { // Already favorited, so remove
            favorites.splice(itemIndex, 1);
        } else { // Not favorited, so add
            favorites.push({ name: itemToToggle.name, path: itemToToggle.path });
        }
        saveToLocalStorage('favoriteItems', favorites);
        refreshDisplayedLists(); // Refresh both lists to update button states
    }

    function refreshDisplayedLists() {
        const favorites = loadFavorites();
        const recentlyViewed = loadRecentlyViewed();

        displayItems(favorites, favoriteItemsListDiv, 'Favorite');
        displayItems(recentlyViewed, recentlyViewedListDiv, 'Recently Viewed');
    }

    // --- Initial Load ---
    refreshDisplayedLists();

    // Expose functions for potential use by index.html (if it needs to directly manipulate these)
    // This is more relevant for adding to recently viewed or toggling favorites from index.html
    window.eveMarketHistory = {
        ...(window.eveMarketHistory || {}), // Preserve existing if any
        addRecentlyViewed: (itemName, itemPath) => {
            if (!itemName || !itemPath) return;
            let recentlyViewed = getFromLocalStorage('recentlyViewedItems');
            const newItem = { name: itemName, path: itemPath };

            // Remove if already exists to move to top
            recentlyViewed = recentlyViewed.filter(item => item.path !== itemPath);

            recentlyViewed.unshift(newItem); // Add to the beginning

            if (recentlyViewed.length > MAX_RECENT_ITEMS) {
                recentlyViewed.length = MAX_RECENT_ITEMS; // Trim to max size
            }
            saveToLocalStorage('recentlyViewedItems', recentlyViewed);
            // If start.html is open in another tab, it won't auto-refresh without complex listeners.
            // For now, it will refresh when start.html is loaded/reloaded.
            // If this script itself is running on start.html, refresh the display:
            if (document.getElementById('recently-viewed-list')) { // Check if on start.html
                 displayItems(recentlyViewed, recentlyViewedListDiv, 'Recently Viewed');
            }
        },
        toggleFavoriteItem: (itemName, itemPath) => {
             if (!itemName || !itemPath) return;
             const item = {name: itemName, path: itemPath};
             toggleFavorite(item);
             // This is for index.html to call. If on start.html, toggleFavorite already refreshes.
        },
        isItemFavorited: (itemPath) => {
            if (!itemPath) return false;
            const favorites = loadFavorites();
            return favorites.some(fav => fav.path === itemPath);
        }
    };
});
