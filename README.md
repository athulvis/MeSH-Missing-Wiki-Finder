# MeSH Missing Wiki Finder

To find the missing Wikidata and Wikipedia articles from the keywords from MeSH Medical dataset.

## Methodology

- Here we use fuzzy search to search wikidata (Using SPARQL Querry service as well as Wikidata API) and find the most matched (with score grater than 80%) and list its QIDs, Instances, Subclasses etc.
- Find the asssociated English wikipedia articles and update the link.
- Separate matched and unmatched outputs according to the matching score (80%).


## Future Enhancements

- Add instance filter. ie, it should be possible to filter out Wikidata items with particular instances (like scientific articles etc.)
- Make a webapp based on this for all type of keyword search.
- Fix existing bugs.

## Authors

- Athul R T 
- Netha Hussain 
- Houcemeddine Turki
