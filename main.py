from bs4 import BeautifulSoup


with open("data.html", "r") as f:
    html_text = f.read()

# Suppose 'html_text' contains all the pasted HTML.
soup = BeautifulSoup(html_text, 'html.parser')

# Extract display text from each <li><a> item
display_list = [a.get_text() for a in soup.find_all('a')]

def swap_comma_text(input_string):
    if ',' in input_string:
        parts = [part.strip() for part in input_string.split(',')]
        # Swap first and second, concatenate rest (if any)
        if len(parts) > 2:
            return ' '.join([parts[1], *parts[2:], parts[0]])
        else:
            return ' '.join([parts[1], parts[0]])
    else:
        return input_string.strip()


# Apply transformation:
result_list = [swap_comma_text(item) for item in display_list]

with open("list_wiki.csv","w") as file:
    file.write("\n".join(result_list))


