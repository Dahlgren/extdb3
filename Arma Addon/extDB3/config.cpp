class CfgPatches
{
	class extDB3
	{
		projectName="extDB3";
		author="Torndeco";
		version="1.017";
		requiredAddons[] = {};
		units[] = {};
	};
};

class CfgFunctions
{
	class extDB3
	{
		class system
		{
			file = "extDB3\system";
			class preInit {preInit = 1;};
		};
	};
};
