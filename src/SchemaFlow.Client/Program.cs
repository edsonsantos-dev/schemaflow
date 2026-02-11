using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using SchemaFlow.Client;
using SchemaFlow.Client.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

var apiBaseUrl = builder.Configuration["ApiBaseUrl"];
if (string.IsNullOrWhiteSpace(apiBaseUrl))
{
    apiBaseUrl = "https://localhost:7102";
}

var options = new ApiEndpointOptions
{
    BaseUri = new Uri(apiBaseUrl, UriKind.Absolute)
};

builder.Services.AddScoped(_ => options);
builder.Services.AddScoped(_ => new HttpClient { BaseAddress = options.BaseUri });
builder.Services.AddScoped<SchemaFlowApiClient>();
builder.Services.AddScoped<MigrationHubClient>();

await builder.Build().RunAsync();
