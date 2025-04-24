using System.Xml.Serialization;
using AngleSharp;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.AI;
using Microsoft.SemanticKernel.Text;
using Textify;

namespace ModernDotNetShowChat.Web.Services.Ingestion;

public class WebIngestionSource : IIngestionSource
{
    private readonly HttpClient _httpClient;

    public WebIngestionSource(string url)
    {
        SourceId = $"{nameof(WebIngestionSource)}:{url}";
        _httpClient = new HttpClient()
        {
            BaseAddress = new Uri(url),
        };
    }

    public string SourceId { get; }
    
    public async Task<IEnumerable<IngestedDocument>> GetNewOrModifiedDocumentsAsync(IQueryable<IngestedDocument> existingDocuments)
    {
        var sitemap = await GetSitemap();

        var results = new List<IngestedDocument>();
        foreach (var entry in sitemap.Entries)
        {
            var sourceFileId = entry.Location;
            var sourceFileVersion = entry.LastModified.ToString("o");

            var existingDocument = await existingDocuments
                .Where(d => d.SourceId == SourceId && d.Id == sourceFileId)
                .FirstOrDefaultAsync();

            if (existingDocument is null)
            {
                results.Add(new() { Id = sourceFileId, Version = sourceFileVersion, SourceId = SourceId });
            }
            else if (existingDocument.Version != sourceFileVersion)
            {
                existingDocument.Version = sourceFileVersion;
                results.Add(existingDocument);
            }
        }

        return results;
    }

    public async Task<IEnumerable<IngestedDocument>> GetDeletedDocumentsAsync(IQueryable<IngestedDocument> existingDocuments)
    {
        var sitemap = await GetSitemap();
        var sourceFileIds = sitemap.Entries.Select(x => x.Location).ToList();
        return await existingDocuments
            .Where(doc => !sourceFileIds.Contains(doc.Id))
            .ToListAsync();
    }

    public async Task<IEnumerable<SemanticSearchRecord>> CreateRecordsForDocumentAsync(IEmbeddingGenerator<string, Embedding<float>> embeddingGenerator, string documentId)
    {
        await using var stream = await _httpClient.GetStreamAsync(documentId);
        var config = Configuration.Default.WithDefaultLoader();
        var context = BrowsingContext.New(config);
        var document = await context.OpenAsync(documentId);
        var converter = new HtmlToTextConverter();
        var output = converter.Convert(document);
        var paragraphs = GetPageParagraphs(output);

        var embeddings = await embeddingGenerator.GenerateAsync(paragraphs.Select(c => c.Text));
        return paragraphs.Zip(embeddings).Select((pair, index) => new SemanticSearchRecord
        {
            Key = $"{documentId}_{pair.First.IndexOnPage}",
            Url = documentId,
            Text = pair.First.Text,
            Vector = pair.Second.Vector,
        });
    }

    private static List<(int IndexOnPage, string Text)> GetPageParagraphs(string pageText)
    {
#pragma warning disable SKEXP0050 // Type is for evaluation purposes only
        return TextChunker.SplitPlainTextParagraphs([pageText], 200)
            .Select((text, index) => (index, text))
            .ToList();
#pragma warning restore SKEXP0050 // Type is for evaluation purposes only
    }
    
    private async Task<Sitemap> GetSitemap()
    {
        var serializer = new XmlSerializer(typeof(Sitemap));
        await using var stream = await _httpClient.GetStreamAsync("sitemap.xml");
        var sitemap = serializer.Deserialize(stream) as Sitemap;
        if (sitemap is null)
        {
            throw new Exception("Unable to read sitemap");
        }

        return sitemap;
    }

    [XmlRoot("urlset", Namespace = "http://www.sitemaps.org/schemas/sitemap/0.9")]
    public class Sitemap
    {
        [XmlElement("url")]
        public required List<Entry> Entries { get; set; }

        public class Entry
        {
            [XmlElement("loc")]
            public required string Location { get; set; }

            [XmlElement("lastmod")]
            public required DateTime LastModified { get; set; }
        }
    }
}

